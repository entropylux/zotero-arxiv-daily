from .base import BaseRetriever, register_retriever
from arxiv import Result as ArxivResult
from ..protocol import Paper
from ..utils import extract_markdown_from_pdf, extract_tex_code_from_tar
from tempfile import TemporaryDirectory
import feedparser
import multiprocessing
import os
from queue import Empty
from typing import Any, Callable, TypeVar
from loguru import logger
import requests
import time
from .. import resource_limits as limits

T = TypeVar("T")

DOWNLOAD_TIMEOUT = (10, 60)
PDF_EXTRACT_TIMEOUT = 180
TAR_EXTRACT_TIMEOUT = 180


def _download_file(url: str, path: str) -> None:
    started = time.monotonic()
    try:
        with requests.get(url, stream=True, timeout=DOWNLOAD_TIMEOUT) as response:
            response.raise_for_status()
            length = response.headers.get("Content-Length")
            if length is not None and int(length) > limits.MAX_DOWNLOAD_BYTES:
                raise limits.ResourceLimitError("Download Content-Length exceeds quota")
            size = 0
            with open(path, "wb") as file:
                for chunk in response.iter_content(chunk_size=64 * 1024):
                    if time.monotonic() - started > 60:
                        raise limits.ResourceLimitError("Download deadline exceeded")
                    size += len(chunk)
                    if size > limits.MAX_DOWNLOAD_BYTES:
                        raise limits.ResourceLimitError("Download byte quota exceeded")
                    file.write(chunk)
    except BaseException:
        if os.path.exists(path):
            os.remove(path)
        raise


def _run_in_subprocess(result_queue, func, args, temp_dir):
    try:
        memory_guard = limits.limit_worker_memory()
        result = func(*args, temp_dir=temp_dir) if temp_dir is not None else func(*args)
        result_queue.put(("ok", limits.bounded_text(result)))
    except Exception as exc:
        result_queue.put(("error", f"{type(exc).__name__}: {str(exc)[:1000]}"))


def _run_with_hard_timeout(
    func: Callable[..., T | None], args: tuple[Any, ...], *, timeout: float,
    operation: str, paper_title: str, temporary_files: bool = False,
) -> T | None:
    # The parent owns the directory so kill/timeout cannot strand downloaded files.
    with TemporaryDirectory(prefix="arxiv-fulltext-") as temp_dir:
        context = multiprocessing.get_context("spawn")
        result_queue = context.Queue(maxsize=1)
        process = context.Process(target=_run_in_subprocess, args=(
            result_queue, func, args, temp_dir if temporary_files else None))
        process.start()
        deadline = time.monotonic() + timeout
        try:
            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    logger.warning(f"{operation} timed out for {paper_title} after {timeout} seconds")
                    return None
                try:
                    status, payload = result_queue.get(timeout=min(0.1, remaining))
                    break
                except Empty:
                    if not process.is_alive():
                        logger.warning(f"{operation} worker exited for {paper_title}")
                        return None
            if status == "ok":
                return limits.bounded_text(payload)
            logger.warning(f"{operation} failed for {paper_title}: {payload}")
            return None
        finally:
            process.join(0.2)
            if process.is_alive():
                process.kill()
            process.join()
            process.close()
            result_queue.close()
            result_queue.join_thread()


def _extract_text_from_pdf_worker(pdf_url: str, *, temp_dir: str) -> str:
    path = os.path.join(temp_dir, "paper.pdf")
    _download_file(pdf_url, path)
    return limits.bounded_text(extract_markdown_from_pdf(path))


def _extract_text_from_html_worker(html_url: str, *, temp_dir: str) -> str | None:
    import trafilatura
    path = os.path.join(temp_dir, "paper.html")
    _download_file(html_url, path)
    with open(path, "rb") as file:
        downloaded = file.read(limits.MAX_DOWNLOAD_BYTES + 1)
    if len(downloaded) > limits.MAX_DOWNLOAD_BYTES:
        raise limits.ResourceLimitError("HTML input quota exceeded")
    text = trafilatura.extract(downloaded, include_comments=False, include_tables=False)
    if not text:
        raise ValueError("No text extracted from HTML")
    return limits.bounded_text(text)


def _extract_text_from_tar_worker(source_url: str, paper_id: str,
                                  paper_title: str | None = None, *, temp_dir: str) -> str | None:
    path = os.path.join(temp_dir, "paper.tar.gz")
    _download_file(source_url, path)
    file_contents = extract_tex_code_from_tar(path, paper_id, paper_title=paper_title)
    if not file_contents or "all" not in file_contents:
        raise ValueError("Main tex file not found.")
    return limits.bounded_text(file_contents["all"])


@register_retriever("arxiv")
class ArxivRetriever(BaseRetriever):
    def __init__(self, config):
        super().__init__(config)
        if self.config.source.arxiv.category is None:
            raise ValueError("category must be specified for arxiv.")

    def _retrieve_raw_papers(self) -> list[ArxivResult]:
        """Read daily metadata from Atom without a second export-API query.

        This bridge populates the Result fields consumed by this application.
        Atom's feed timestamps are NOT treated as paper submission/update dates;
        Result's unused date fields retain their library defaults.
        """
        import re
        from html.parser import HTMLParser

        class TextExtractor(HTMLParser):
            def __init__(self):
                super().__init__(convert_charrefs=True)
                self.parts = []

            def handle_starttag(self, tag, attrs):
                if tag in {"p", "br", "div"}:
                    self.parts.append("\n")

            def handle_endtag(self, tag):
                if tag in {"p", "div"}:
                    self.parts.append("\n")

            def handle_data(self, data):
                self.parts.append(data)

        def text_field(entry, key):
            value = str(entry.get(key, ""))
            kind = str(entry.get(f"{key}_detail", {}).get("type", "text/plain"))
            if "html" in kind:
                parser = TextExtractor()
                parser.feed(value)
                parser.close()
                value = "".join(parser.parts)
            return value.strip()

        def split_creator(value):
            # dc:creator is comma-separated. Preserve commas in affiliations.
            names, current, depth = [], [], 0
            for char in value:
                if char in "([":
                    depth += 1
                elif char in ")]":
                    depth = max(0, depth - 1)
                if char == "," and depth == 0:
                    names.append("".join(current).strip())
                    current = []
                else:
                    current.append(char)
            names.append("".join(current).strip())
            merged = []
            for name in filter(None, names):
                if name.lower().rstrip(".") in {"jr", "sr", "ii", "iii", "iv"} and merged:
                    merged[-1] += ", " + name
                else:
                    merged.append(name)
            return merged

        query = "+".join(self.config.source.arxiv.category)
        url = f"https://rss.arxiv.org/atom/{query}"
        logger.info(f"Fetching arXiv Atom metadata: {query}")
        # One bounded request; do not repeatedly hammer a throttled endpoint.
        with requests.get(
            url,
            timeout=DOWNLOAD_TIMEOUT,
            headers={"User-Agent": "zotero-arxiv-daily/Atom-metadata"},
        ) as response:
            if response.status_code == 429:
                retry_after = response.headers.get("Retry-After", "not specified")
                raise RuntimeError(
                    "arXiv Atom feed returned HTTP 429; stopping without further "
                    f"requests. Retry-After: {retry_after}. Retry later."
                )
            response.raise_for_status()
            feed = feedparser.parse(response.content)

        if not feed.get("version"):
            raise RuntimeError("arXiv returned a non-feed response, not a valid Atom feed.")
        if "Feed error for query" in feed.feed.get("title", ""):
            raise ValueError(f"Invalid ARXIV_QUERY: {query}.")
        if feed.get("bozo"):
            raise RuntimeError(f"Malformed arXiv Atom feed: {feed.get('bozo_exception')}")

        include_cross = self.config.source.arxiv.get("include_cross_list", False)
        allowed = {"new", "cross"} if include_cross else {"new"}
        raw_papers, seen = [], set()
        identifier_pattern = re.compile(
            r"(?:\d{4}\.\d{4,5}|[A-Za-z][A-Za-z0-9.-]*/\d{7})(?:v\d+)?"
        )
        for entry in feed.entries:
            if entry.get("arxiv_announce_type", "new") not in allowed:
                continue
            paper_id = str(entry.get("id", "")).removeprefix("oai:arXiv.org:")
            if "/abs/" in paper_id:
                paper_id = paper_id.split("/abs/", 1)[1]
            if not identifier_pattern.fullmatch(paper_id):
                raise ValueError(f"Unrecognized arXiv Atom identifier: {paper_id!r}")
            if paper_id in seen:
                continue
            seen.add(paper_id)

            title = " ".join(text_field(entry, "title").split())
            abstract = text_field(entry, "summary")
            # Atom's summary prepends ID and announcement metadata, unlike API summary.
            abstract = re.sub(
                r"\A\s*arXiv:\S+\s+Announce Type:\s*\S+\s+Abstract:\s*",
                "", abstract, count=1, flags=re.IGNORECASE,
            )
            if not title or not abstract:
                raise ValueError(f"Missing title or abstract in Atom entry {paper_id}")

            author_fields = [a.get("name", "").strip() for a in entry.get("authors", [])]
            author_fields = [name for name in author_fields if name]
            if not author_fields:
                author_fields = [str(entry.get("author") or entry.get("dc_creator") or "")]
            # feedparser maps dc:creator to one flattened author entry.
            names = split_creator(author_fields[0]) if len(author_fields) == 1 else author_fields
            if not names:
                logger.warning(f"No authors supplied in Atom entry {paper_id}")
            categories = [t["term"] for t in entry.get("tags", []) if t.get("term")]
            raw_papers.append(ArxivResult(
                entry_id=f"https://arxiv.org/abs/{paper_id}",
                title=title,
                authors=[ArxivResult.Author(name) for name in names],
                summary=abstract,
                categories=categories,
                # Do not infer an undocumented primary-category ordering.
                primary_category="",
                journal_ref=entry.get("arxiv_journal_reference", ""),
                doi=entry.get("arxiv_doi", ""),
                links=[ArxivResult.Link(
                    href=f"https://arxiv.org/pdf/{paper_id}",
                    title="pdf", rel="related", content_type="application/pdf",
                )],
            ))
            if self.config.executor.debug and len(raw_papers) >= 10:
                break

        logger.info(
            f"Loaded {len(raw_papers)} arXiv paper metadata records from Atom; "
            "export.arxiv.org API was not used."
        )
        return raw_papers

    def retrieve_metadata(self) -> list[Paper]:
        raw_papers = self._retrieve_raw_papers()
        self._raw_by_url = {p.entry_id: p for p in raw_papers}
        return [self._metadata_to_paper(p) for p in raw_papers]

    def enrich_paper(self, paper: Paper) -> None:
        raw_paper = self._raw_by_url[paper.url]
        paper.full_text = self._extract_full_text(raw_paper)

    @staticmethod
    def _extract_full_text(raw_paper: ArxivResult) -> str | None:
        full_text = extract_text_from_tar(raw_paper)
        if full_text is None:
            full_text = extract_text_from_html(raw_paper)
        if full_text is None:
            full_text = extract_text_from_pdf(raw_paper)
        return full_text

    def convert_to_paper(self, raw_paper: ArxivResult) -> Paper:
        paper = self._metadata_to_paper(raw_paper)
        paper.full_text = self._extract_full_text(raw_paper)
        return paper

    def _metadata_to_paper(self, raw_paper: ArxivResult) -> Paper:
        title = raw_paper.title
        authors = [a.name for a in raw_paper.authors]
        abstract = raw_paper.summary
        pdf_url = raw_paper.pdf_url
        return Paper(
            source=self.name,
            title=title,
            authors=authors,
            abstract=abstract,
            url=raw_paper.entry_id,
            pdf_url=pdf_url,
            full_text=None,
        )


def extract_text_from_html(paper: ArxivResult) -> str | None:
    html_url = paper.entry_id.replace("/abs/", "/html/")
    return _run_with_hard_timeout(
        _extract_text_from_html_worker, (html_url,), timeout=90,
        operation="HTML extraction", paper_title=paper.title, temporary_files=True,
    )


def extract_text_from_pdf(paper: ArxivResult) -> str | None:
    if paper.pdf_url is None:
        logger.warning(f"No PDF URL available for {paper.title}")
        return None
    return _run_with_hard_timeout(
        _extract_text_from_pdf_worker,
        (paper.pdf_url,),
        timeout=PDF_EXTRACT_TIMEOUT,
        operation="PDF extraction", temporary_files=True,
        paper_title=paper.title,
    )


def extract_text_from_tar(paper: ArxivResult) -> str | None:
    source_url = paper.source_url()
    if source_url is None:
        logger.warning(f"No source URL available for {paper.title}")
        return None
    return _run_with_hard_timeout(
        _extract_text_from_tar_worker,
        (source_url, paper.entry_id, paper.title),
        timeout=TAR_EXTRACT_TIMEOUT,
        operation="Tar extraction", temporary_files=True,
        paper_title=paper.title,
    )
