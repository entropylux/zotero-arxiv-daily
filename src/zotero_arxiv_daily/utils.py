import tarfile
import re
import glob
import math
import smtplib
import ssl
from collections import Counter
from email.header import Header
from email.mime.text import MIMEText
from email.utils import parseaddr, formataddr
from loguru import logger
import datetime
from omegaconf import DictConfig
import gzip
import io
from . import resource_limits as limits

_TOKEN_RE = re.compile(r'[a-zA-Z0-9]+')


def _strip_tex_comments(text: str) -> str:
    """Clean inert TeX text without losing escaped percentages or line endings."""
    # A percent sign starts a comment only after an even number of backslashes.
    # Keep those pairs (TeX line breaks) so later cleanup preserves separation.
    return re.sub(r'(?<!\\)((?:\\\\)*)%[^\r\n]*', r'\1', text)


def _tokenize(text: str) -> list[str]:
    tokens = []
    for match in _TOKEN_RE.finditer(text):
        if len(tokens) >= limits.MAX_TOKENS:
            raise limits.ResourceLimitError("TeX token quota exceeded")
        tokens.append(match.group().lower())
    return tokens


def _bm25_pick(query: str, candidates: dict[str, str], k1: float = 1.5, b: float = 0.75) -> str:
    """Return the candidate key whose content best matches *query* by BM25."""
    query_tokens = _tokenize(query)
    if not query_tokens:
        return next(iter(candidates))

    doc_tokens = {name: _tokenize(content) for name, content in candidates.items()}
    N = len(doc_tokens)
    avgdl = sum(len(t) for t in doc_tokens.values()) / max(N, 1)

    df: Counter[str] = Counter()
    for tokens in doc_tokens.values():
        df.update(set(tokens))

    best_name, best_score = None, -1.0
    for name, tokens in doc_tokens.items():
        tf = Counter(tokens)
        dl = len(tokens)
        score = 0.0
        for q in query_tokens:
            n_q = df.get(q, 0)
            idf = math.log((N - n_q + 0.5) / (n_q + 0.5) + 1)
            f_q = tf.get(q, 0)
            score += idf * (f_q * (k1 + 1)) / (f_q + k1 * (1 - b + b * dl / max(avgdl, 1)))
        if score > best_score:
            best_score = score
            best_name = name
    return best_name


def extract_tex_code_from_tar(file_path:str, paper_id:str, paper_title:str | None = None) -> dict[str,str]:
    # Bound the complete decompressed stream, including PAX headers and skipped
    # members, before tarfile is allowed to allocate metadata or scan the archive.
    with open(file_path, "rb") as source:
        compressed = source.read(limits.MAX_DOWNLOAD_BYTES + 1)
    if len(compressed) > limits.MAX_DOWNLOAD_BYTES:
        raise limits.ResourceLimitError("Source archive download quota exceeded")
    if compressed.startswith(b"\x1f\x8b"):
        with gzip.GzipFile(fileobj=io.BytesIO(compressed)) as source:
            raw = source.read(limits.MAX_ARCHIVE_BYTES + 1)
    else:
        raw = compressed
    if len(raw) > limits.MAX_ARCHIVE_BYTES:
        raise limits.ResourceLimitError("Archive expansion quota exceeded")
    try:
        with tarfile.open(fileobj=io.BytesIO(raw), mode="r:") as tar:
            contents = {}
            tex_bytes = 0
            for count, member in enumerate(tar, 1):
                if count > limits.MAX_ARCHIVE_MEMBERS:
                    raise limits.ResourceLimitError("Archive member quota exceeded")
                if member.size > limits.MAX_MEMBER_BYTES or member.size < 0:
                    raise limits.ResourceLimitError("Archive member size quota exceeded")
                if not member.isfile() or not member.name.endswith((".tex", ".bbl", ".bib")):
                    continue
                if member.name in contents:
                    raise limits.ResourceLimitError("Duplicate archive member")
                tex_bytes += member.size
                if tex_bytes > limits.MAX_TEX_BYTES:
                    raise limits.ResourceLimitError("Total TeX size quota exceeded")
                with tar.extractfile(member) as source:
                    data = source.read(limits.MAX_MEMBER_BYTES + 1)
                if len(data) > limits.MAX_MEMBER_BYTES:
                    raise limits.ResourceLimitError("TeX member quota exceeded")
                contents[member.name] = data.decode("utf-8", errors="ignore")
    except tarfile.ReadError:
        logger.debug(f"Failed to find main tex file of {paper_id}: Not a tar file.")
        return None

    tex_files = [name for name in contents if name.endswith('.tex')]
    if len(tex_files) == 0:
        logger.debug(f"Failed to find main tex file of {paper_id}: No tex file.")
        return None
    
    bbl_file = [f for f in contents if f.endswith('.bbl')]
    match len(bbl_file) :
        case 0:
            if len(tex_files) > 1:
                logger.debug(f"Cannot find main tex file of {paper_id} from bbl: There are multiple tex files while no bbl file.")
                main_tex = None
            else:
                main_tex = tex_files[0]
        case 1:
            main_name = bbl_file[0].replace('.bbl','')
            main_tex = f"{main_name}.tex"
            if main_tex not in tex_files:
                logger.debug(f"Cannot find main tex file of {paper_id} from bbl: The bbl file does not match any tex file.")
                main_tex = None
        case _:
            logger.debug(f"Cannot find main tex file of {paper_id} from bbl: There are multiple bbl files.")
            main_tex = None

    if main_tex is None:
        logger.debug(f"Trying to choose tex file containing the document block as main tex file of {paper_id}")

    file_contents = {}
    doc_block_candidates: list[str] = []
    for t in tex_files:
        content = contents[t]
        content = _strip_tex_comments(content)
        content = re.sub(r'\\begin{comment}.*?\\end{comment}', '', content, flags=re.DOTALL)
        content = re.sub(r'\\iffalse.*?\\fi', '', content, flags=re.DOTALL)
        content = re.sub(r'\n+', '\n', content)
        content = re.sub(r'\\\\', ' ', content)
        content = re.sub(r'[ \t\r\f]{3,}', ' ', content)
        if main_tex is None and re.search(r'\\begin\{document\}', content) and not any(w in t for w in ['example', 'sample', 'template']):
            doc_block_candidates.append(t)
        file_contents[t] = content

    if main_tex is None:
        if len(doc_block_candidates) == 1:
            main_tex = doc_block_candidates[0]
            logger.debug(f"Choose {main_tex} as main tex file of {paper_id}")
        elif len(doc_block_candidates) > 1:
            if paper_title:
                main_tex = _bm25_pick(paper_title, {c: file_contents[c] for c in doc_block_candidates})
                logger.debug(f"Multiple document blocks found in {paper_id}; BM25 selected {main_tex} from {doc_block_candidates}")
            else:
                main_tex = doc_block_candidates[0]
                logger.debug(f"Multiple document blocks found in {paper_id}; no title provided, using first candidate {main_tex}")

    if main_tex is not None:
        main_source:str = file_contents[main_tex]
        # Resolve one layer once. Never re-expand text introduced by a replacement.
        include_pattern = re.compile(r'\\(?:input|include)\{([^{}]+)\}')
        matches = list(include_pattern.finditer(main_source))
        if len(matches) > limits.MAX_INCLUDES:
            raise limits.ResourceLimitError("TeX include quota exceeded")
        parts, offset, size = [], 0, 0
        for match in matches:
            name = match.group(1)
            name = name if name.endswith('.tex') else name + '.tex'
            prefix, replacement = main_source[offset:match.start()], file_contents.get(name, '')
            size += len(prefix) + len(replacement)
            if size > limits.MAX_TEXT_CHARS:
                raise limits.ResourceLimitError("Expanded TeX output quota exceeded")
            parts.extend((prefix, replacement))
            offset = match.end()
        tail = main_source[offset:]
        if size + len(tail) > limits.MAX_TEXT_CHARS:
            raise limits.ResourceLimitError("Expanded TeX output quota exceeded")
        main_source = ''.join(parts) + tail
        # Preserve bibliography data as inert text. Previously only its filename
        # helped choose the main TeX file, leaving citation keys unresolved.
        bibliography = '\n'.join(contents[name] for name in contents
                                 if name.endswith(('.bbl', '.bib')))
        if bibliography:
            main_source += '\n[REFERENCE SOURCE FILES]\n' + bibliography
        file_contents["all"] = limits.bounded_text(main_source)
    else:
        logger.debug(f"Failed to find main tex file of {paper_id}: No tex file containing the document block.")
        file_contents["all"] = None
        
    return file_contents

def extract_markdown_from_pdf(file_path:str) -> str:
    import pymupdf
    import pymupdf.layout
    import pymupdf4llm
    pymupdf.TOOLS.mupdf_display_errors(False)
    pymupdf.layout.activate()
    with pymupdf.open(file_path) as document:
        if len(document) > limits.MAX_PDF_PAGES:
            raise limits.ResourceLimitError("PDF page quota exceeded")
        parts, size = [], 0
        for page in range(len(document)):
            text = pymupdf4llm.to_markdown(document, pages=[page], use_ocr=False,
                                         header=False, footer=False, ignore_code=True)
            size += len(text)
            if size > limits.MAX_TEXT_CHARS:
                raise limits.ResourceLimitError("PDF output quota exceeded")
            parts.append(text)
        return ''.join(parts)

def glob_match(path:str, pattern:str) -> bool:
    re_pattern = glob.translate(pattern,recursive=True)
    return re.match(re_pattern, path) is not None

def send_email(config:DictConfig, html:str):
    sender = config.email.sender
    receiver = config.email.receiver
    password = config.email.sender_password
    smtp_server = config.email.smtp_server
    smtp_port = config.email.smtp_port
    def _format_addr(s):
        name, addr = parseaddr(s)
        return formataddr((Header(name, 'utf-8').encode(), addr))

    msg = MIMEText(html, 'html', 'utf-8')
    msg['From'] = _format_addr('Github Action <%s>' % sender)
    msg['To'] = _format_addr('You <%s>' % receiver)
    today = datetime.datetime.now().strftime('%Y/%m/%d')
    msg['Subject'] = Header(f'Daily arXiv {today}', 'utf-8').encode()

    context = ssl.create_default_context()
    mode = config.email.get("tls_mode", "auto")
    if mode not in {"auto", "implicit", "starttls"}:
        raise ValueError("email.tls_mode must be auto, implicit, or starttls")
    implicit = mode == "implicit" or (mode == "auto" and int(smtp_port) == 465)
    if implicit:
        server = smtplib.SMTP_SSL(smtp_server, smtp_port, timeout=30, context=context)
    else:
        server = smtplib.SMTP(smtp_server, smtp_port, timeout=30)
    try:
        if not implicit:
            server.starttls(context=context)
        # Never authenticate after failed TLS negotiation or certificate verification.
        server.login(sender, password)
        server.sendmail(sender, [receiver], msg.as_string())
        server.quit()
    finally:
        server.close()
