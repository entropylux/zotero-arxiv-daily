"""Tests for ArxivRetriever."""

import time
from pathlib import Path
from types import SimpleNamespace
from xml.sax.saxutils import escape

import arxiv
import feedparser
import pytest
import requests

from zotero_arxiv_daily.retriever.arxiv_retriever import ArxivRetriever, _run_with_hard_timeout
import zotero_arxiv_daily.retriever.arxiv_retriever as arxiv_retriever


def _sleep_and_return(value: str, delay_seconds: float) -> str:
    time.sleep(delay_seconds)
    return value


def _raise_runtime_error() -> None:
    raise RuntimeError("boom")


@pytest.fixture(autouse=True)
def no_export_api(monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail("Metadata retrieval must not query the arXiv export API")
    monkeypatch.setattr(arxiv, "Client", forbidden)
    monkeypatch.setattr(arxiv, "Search", forbidden)


@pytest.fixture
def serve_feed(monkeypatch):
    def serve(content, status=200, headers=None):
        calls = []
        class Response:
            status_code = status

            def __init__(self):
                self.content = content.encode() if isinstance(content, str) else content
                self.headers = headers or {}

            def __enter__(self):
                return self

            def __exit__(self, *args):
                pass

            def raise_for_status(self):
                if self.status_code >= 400:
                    raise requests.HTTPError(str(self.status_code))

        def get(url, **kwargs):
            assert url == "https://rss.arxiv.org/atom/cs.AI+cs.CV"
            assert kwargs["timeout"] == (10, 60)
            calls.append(url)
            return Response()

        monkeypatch.setattr(arxiv_retriever.requests, "get", get)
        return calls
    return serve


def atom_entry(pid="2609.12345v1", announce="new", title="A paper", abstract="A < B & C.",
               creator="Alice, Bob", extra=""):
    return f'''<entry><id>{escape(pid)}</id><title>{escape(title)}</title>
    <summary>{escape(f'arXiv:{pid} Announce Type: {announce} Abstract: ' + abstract)}</summary>
    <arxiv:announce_type>{announce}</arxiv:announce_type>
    <dc:creator>{escape(creator)}</dc:creator><category term="cs.AI"/>{extra}</entry>'''


def atom_feed(entries="", title="arXiv updates"):
    return f'''<?xml version="1.0" encoding="UTF-8"?>
    <feed xmlns="http://www.w3.org/2005/Atom"
          xmlns:arxiv="http://arxiv.org/schemas/atom"
          xmlns:dc="http://purl.org/dc/elements/1.1/">
    <title>{escape(title)}</title>{entries}</feed>'''


def test_arxiv_retriever(config, serve_feed, monkeypatch):
    monkeypatch.setattr("zotero_arxiv_daily.retriever.base.sleep", lambda _: None)
    content = Path(__file__).with_name("arxiv_rss_example.xml").read_bytes()
    calls = serve_feed(content)
    new_entries = [
        e for e in feedparser.parse(content).entries
        if e.get("arxiv_announce_type", "new") == "new"
    ]

    # Skip file downloads in convert_to_paper
    monkeypatch.setattr(arxiv_retriever, "extract_text_from_html", lambda paper: None)
    monkeypatch.setattr(arxiv_retriever, "extract_text_from_pdf", lambda paper: None)
    monkeypatch.setattr(arxiv_retriever, "extract_text_from_tar", lambda paper: None)

    retriever = ArxivRetriever(config)
    papers = retriever.retrieve_papers()

    assert len(papers) == len(new_entries)
    assert set(p.title for p in papers) == set(e.title for e in new_entries)
    assert len(calls) == 1
    assert all(p.authors and not p.abstract.startswith("arXiv:") for p in papers)


@pytest.mark.parametrize("include_cross, expected", [(False, 1), (True, 2)])
def test_announcement_filter_and_dedup(config, serve_feed, include_cross, expected):
    config.source.arxiv.include_cross_list = include_cross
    entries = atom_entry() * 2 + atom_entry("2609.12346", "cross")
    entries += atom_entry("2609.12347", "replace")
    serve_feed(atom_feed(entries))
    assert len(ArxivRetriever(config)._retrieve_raw_papers()) == expected


@pytest.mark.parametrize("pid", ["oai:arXiv.org:2609.12345v1", "https://arxiv.org/abs/2609.12345v1", "hep-th/9901001v2"])
def test_metadata_and_download_urls(config, serve_feed, pid):
    serve_feed(atom_feed(atom_entry(pid, creator="Alice (Lab, University), Bob, Jr., Chloé")))
    paper, = ArxivRetriever(config)._retrieve_raw_papers()
    normalized = pid.removeprefix("oai:arXiv.org:").removeprefix("https://arxiv.org/abs/")
    assert paper.summary == "A < B & C."
    assert paper.title == "A paper"
    assert [a.name for a in paper.authors] == ["Alice (Lab, University)", "Bob, Jr.", "Chloé"]
    assert paper.categories == ["cs.AI"]
    assert paper.entry_id == f"https://arxiv.org/abs/{normalized}"
    assert paper.pdf_url == f"https://arxiv.org/pdf/{normalized}"
    assert paper.source_url() == f"https://arxiv.org/src/{normalized}"


def test_html_summary(config, serve_feed):
    entry = atom_entry().replace(
        "<summary>", "<summary type=\"html\">"
    ).replace("Abstract: ", "&lt;p&gt;Abstract: ").replace("</summary>", "&lt;/p&gt;</summary>")
    serve_feed(atom_feed(entry))
    assert ArxivRetriever(config)._retrieve_raw_papers()[0].summary == "A < B & C."


def test_debug_limit(config, serve_feed):
    config.executor.debug = True
    serve_feed(atom_feed("".join(atom_entry(f"2609.{i:05}") for i in range(12))))
    assert len(ArxivRetriever(config)._retrieve_raw_papers()) == 10


def test_empty_feed(config, serve_feed):
    serve_feed(atom_feed())
    assert ArxivRetriever(config)._retrieve_raw_papers() == []


def test_metadata_does_not_download_fulltext(config, serve_feed, monkeypatch):
    serve_feed(atom_feed(atom_entry()))
    calls = []
    monkeypatch.setattr(arxiv_retriever, "extract_text_from_tar", lambda paper: calls.append(paper.entry_id) or "full text")
    retriever = ArxivRetriever(config)
    papers = retriever.retrieve_metadata()
    assert calls == []
    assert papers[0].full_text is None
    papers[0].score = 7.0
    retriever.enrich_paper(papers[0])
    assert calls == [papers[0].url]
    assert papers[0].full_text == "full text"
    assert papers[0].score == 7.0


@pytest.mark.parametrize("content, error", [
    ("<html>upstream error</html>", "non-feed"),
    (atom_feed()[:-7], "Malformed"),
    (atom_feed(title="Feed error for query"), "Invalid ARXIV_QUERY"),
    (atom_feed(atom_entry("invalid")), "identifier"),
    (atom_feed(atom_entry(title="")), "Missing title"),
])
def test_bad_feeds_fail_explicitly(config, serve_feed, content, error):
    serve_feed(content)
    with pytest.raises((ValueError, RuntimeError), match=error):
        ArxivRetriever(config)._retrieve_raw_papers()


def test_rate_limit_does_not_retry(config, serve_feed):
    calls = serve_feed("", status=429, headers={"Retry-After": "120"})
    with pytest.raises(RuntimeError, match="Retry-After: 120"):
        ArxivRetriever(config)._retrieve_raw_papers()
    assert len(calls) == 1


def test_http_failure(config, serve_feed):
    serve_feed("", status=503)
    with pytest.raises(requests.HTTPError):
        ArxivRetriever(config)._retrieve_raw_papers()


def test_run_with_hard_timeout_returns_value():
    # Windows spawn imports the module's PDF dependencies before running the worker.
    result = _run_with_hard_timeout(
        _sleep_and_return, ("done", 0.01), timeout=15, operation="test op", paper_title="paper"
    )
    assert result == "done"


def test_run_with_hard_timeout_returns_none_on_timeout(monkeypatch):
    warnings: list[str] = []
    monkeypatch.setattr(arxiv_retriever, "logger", SimpleNamespace(warning=warnings.append))
    result = _run_with_hard_timeout(
        _sleep_and_return, ("done", 1.0), timeout=0.01, operation="test op", paper_title="paper"
    )
    assert result is None
    assert "timed out" in warnings[0]


def test_run_with_hard_timeout_returns_none_on_failure(monkeypatch):
    warnings: list[str] = []
    monkeypatch.setattr(arxiv_retriever, "logger", SimpleNamespace(warning=warnings.append))
    result = _run_with_hard_timeout(
        _raise_runtime_error, (), timeout=15, operation="test op", paper_title="paper"
    )
    assert result is None
    assert "boom" in warnings[0]
