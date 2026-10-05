"""Offline regressions for the six repository-scan findings."""
import gzip
import io
import json
from pathlib import Path
import re
import smtplib
import ssl
import subprocess
import sys
import tarfile
import time
from html.parser import HTMLParser
from types import SimpleNamespace

import pytest
import yaml

from tests.canned_responses import make_sample_paper, make_stub_smtp
from zotero_arxiv_daily import resource_limits as limits
from zotero_arxiv_daily.construct_email import render_email, _safe_pdf_url
from zotero_arxiv_daily.protocol import Paper
from zotero_arxiv_daily.reranker.prestige import apply_prestige_bonus
from zotero_arxiv_daily.retriever import arxiv_retriever as retriever
from zotero_arxiv_daily.utils import extract_tex_code_from_tar, send_email


def test_workflows_pin_actions_and_limit_permissions():
    root = Path(__file__).resolve().parents[1]
    for path in (root / '.github/workflows').glob('*.yml'):
        config = yaml.safe_load(path.read_text(encoding='utf-8'))
        assert config['permissions'] == {'contents': 'read'}
        for job in config['jobs'].values():
            for step in job['steps']:
                if 'uses' in step:
                    assert re.fullmatch(r'[^@]+@[0-9a-f]{40}', step['uses'])
                if step.get('uses', '').startswith('actions/checkout@') and path.name != 'keep-alive.yml':
                    assert step['with']['persist-credentials'] is False


def test_implicit_tls_verifies_identity(config, monkeypatch):
    sent = []
    config.email.smtp_port = 465
    base = make_stub_smtp(sent)
    class SMTP(base):
        def __init__(self, *args, context, timeout):
            assert context.check_hostname
            assert context.verify_mode == ssl.CERT_REQUIRED
            assert timeout == 30
            raise ssl.SSLCertVerificationError('untrusted server')
    monkeypatch.setattr(smtplib, 'SMTP_SSL', SMTP)
    monkeypatch.setattr(smtplib, 'SMTP', lambda *a, **k: pytest.fail('No fallback allowed'))
    with pytest.raises(ssl.SSLCertVerificationError):
        send_email(config, 'message')
    assert not sent


@pytest.mark.parametrize('url', [
    'javascript:alert(1)', 'http://arxiv.org/pdf/1', 'https://arxiv.org.evil.test/pdf/1',
    'https://arxiv.org@evil.test/1', 'https://evil.test@arxiv.org/1',
    'https://arxiv.org:9999/pdf/1', None, 'https://[bad',
])
def test_unsafe_links_are_inert(url):
    assert _safe_pdf_url(url) == '#'


def test_all_remote_html_fields_are_text():
    payload = '<img src="https://evil.test/x" onerror="alert(1)"><script>alert(1)</script>&lt;b&gt;'
    paper = make_sample_paper(title=payload, authors=[payload], tldr=payload,
                              affiliations=[payload], score=7,
                              pdf_url='https://arxiv.org/pdf/1" onclick="alert(1)')
    document = render_email([paper])
    class Elements(HTMLParser):
        def handle_starttag(self, tag, attrs):
            assert tag not in {'script', 'img', 'iframe', 'form', 'b'}
            assert not any(k.startswith('on') for k, _ in attrs)
            for key, value in attrs:
                if key == 'href':
                    assert value.startswith('https://arxiv.org/pdf/')
    Elements().feed(document)
    assert document.count('&lt;img') == 4
    assert '&amp;lt;b&amp;gt;' in document


def test_injected_affiliations_never_change_final_order():
    forged = make_sample_paper(score=6.0, affiliations=['Harvard University', 'MIT'])
    legitimate = make_sample_paper(score=6.05)
    result = apply_prestige_bonus([legitimate, forged],
                                  {'bonus': 0.1, 'institutions': ['Harvard University', 'MIT']})
    assert result == [legitimate, forged]
    assert forged.score == 6.0


@pytest.mark.parametrize('response', ['[1]', '{"name":"MIT"}', '[[]]', '["MIT"] trailing',
                                    json.dumps(['a' * 257]), json.dumps(['MIT'] * 33), 'x' * 8193])
def test_affiliation_schema_fails_closed(monkeypatch, response):
    import zotero_arxiv_daily.protocol as protocol
    monkeypatch.setattr(protocol, '_request_llm', lambda *a, **k: response)
    # Avoid network-dependent tokenizer cache in this parser test.
    monkeypatch.setattr(protocol.tiktoken, 'encoding_for_model',
                        lambda *a: SimpleNamespace(encode=lambda s: list(s), decode=lambda s: ''.join(s)))
    assert make_sample_paper().generate_affiliations(None, {}) is None


@pytest.mark.parametrize('declared', [None, '9'])
def test_stream_limits_remove_partial_download(tmp_path, monkeypatch, declared):
    monkeypatch.setattr(limits, 'MAX_DOWNLOAD_BYTES', 8)
    class Response:
        headers = {} if declared is None else {'Content-Length': declared}
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def raise_for_status(self): pass
        def iter_content(self, **kwargs): yield b'12345'; yield b'6789'
    monkeypatch.setattr(retriever.requests, 'get', lambda *a, **k: Response())
    output = tmp_path / 'download'
    with pytest.raises(limits.ResourceLimitError):
        retriever._download_file('https://arxiv.org/test', str(output))
    assert not output.exists()


def archive(tmp_path, files):
    stream = io.BytesIO()
    with tarfile.open(fileobj=stream, mode='w') as tar:
        for name, content in files:
            member = tarfile.TarInfo(name)
            member.size = len(content)
            tar.addfile(member, io.BytesIO(content))
    path = tmp_path / 'source.gz'
    path.write_bytes(gzip.compress(stream.getvalue()))
    return str(path)


def test_archive_expansion_is_bounded_before_tar_parse(tmp_path, monkeypatch):
    path = archive(tmp_path, [('main.tex', b'x' * 10000)])
    monkeypatch.setattr(limits, 'MAX_ARCHIVE_BYTES', 1024)
    with pytest.raises(limits.ResourceLimitError, match='expansion'):
        extract_tex_code_from_tar(path, 'test')


@pytest.mark.parametrize('quota,value,files', [
    ('MAX_MEMBER_BYTES', 8, [('main.tex', b'x' * 9)]),
    ('MAX_ARCHIVE_MEMBERS', 1, [('a.tex', b'1'), ('b.tex', b'2')]),
    ('MAX_TEX_BYTES', 3, [('a.tex', b'12'), ('b.tex', b'34')]),
    ('MAX_TEXT_CHARS', 40, [('main.tex', b'\\begin{document}\\input{a}\\input{a}'), ('a.tex', b'x' * 20)]),
])
def test_archive_quotas(tmp_path, monkeypatch, quota, value, files):
    path = archive(tmp_path, files)
    monkeypatch.setattr(limits, quota, value)
    with pytest.raises(limits.ResourceLimitError):
        extract_tex_code_from_tar(path, 'test')


def _oversized_output():
    return 'x' * (limits.MAX_TEXT_CHARS + 1)


def _leave_file_then_sleep(marker, *, temp_dir):
    Path(marker).write_text(temp_dir, encoding='utf-8')
    (Path(temp_dir) / 'partial').write_bytes(b'partial')
    time.sleep(30)


def test_oversized_output_never_crosses_worker_boundary():
    assert retriever._run_with_hard_timeout(_oversized_output, (), timeout=15,
                                           operation='output', paper_title='test') is None


def test_parent_cleans_files_after_terminated_worker(tmp_path):
    marker = tmp_path / 'marker'
    result = retriever._run_with_hard_timeout(_leave_file_then_sleep, (str(marker),), timeout=8,
                                             operation='cleanup', paper_title='test', temporary_files=True)
    assert result is None
    assert marker.exists(), 'Worker must have reached the download stage'
    assert not Path(marker.read_text(encoding='utf-8')).exists()


def test_native_memory_limit_blocks_excess_allocation():
    # Test one guard in a fresh lightweight process. Installing a second,
    # tighter Windows job inside the already guarded parser tests job nesting,
    # not the production execution path.
    probe = '''
from zotero_arxiv_daily import resource_limits as limits
limits.MAX_WORKER_MEMORY = 128 * 1024 * 1024
guard = limits.limit_worker_memory()
try:
    allocation = bytearray(192 * 1024 * 1024)
except MemoryError:
    print('blocked')
else:
    print('unguarded')
'''
    result = subprocess.run([sys.executable, '-c', probe], capture_output=True,
                            text=True, timeout=15, check=True)
    assert result.stdout.strip() == 'blocked'


def test_html_download_uses_quota_helper(tmp_path, monkeypatch):
    import trafilatura
    calls = []
    def download(url, path):
        calls.append(url)
        Path(path).write_bytes(b'<article>text</article>')
    monkeypatch.setattr(retriever, '_download_file', download)
    monkeypatch.setattr(trafilatura, 'extract', lambda *a, **k: 'x' * (limits.MAX_TEXT_CHARS + 1))
    with pytest.raises(limits.ResourceLimitError):
        retriever._extract_text_from_html_worker('https://arxiv.org/html/test', temp_dir=str(tmp_path))
    assert calls == ['https://arxiv.org/html/test']


def test_pdf_page_limit_precedes_text_conversion(tmp_path, monkeypatch):
    import pymupdf
    import pymupdf4llm
    from zotero_arxiv_daily.utils import extract_markdown_from_pdf
    path = tmp_path / 'pages.pdf'
    with pymupdf.open() as document:
        document.new_page()
        document.new_page()
        document.save(path)
    monkeypatch.setattr(limits, 'MAX_PDF_PAGES', 1)
    monkeypatch.setattr(pymupdf4llm, 'to_markdown', lambda *a, **k: pytest.fail('Parse after quota exceeded'))
    with pytest.raises(limits.ResourceLimitError, match='page quota'):
        extract_markdown_from_pdf(str(path))
