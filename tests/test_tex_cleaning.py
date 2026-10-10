"""Scientific values must survive inert TeX cleanup for evidence review."""
import io
import tarfile

import pytest

from zotero_arxiv_daily.utils import _strip_tex_comments, extract_tex_code_from_tar


@pytest.mark.parametrize(
    ("source", "expected"),
    [
        ("result % comment at EOF", "result "),
        (r"5.1\% to 10.0\% improvement", r"5.1\% to 10.0\% improvement"),
        (r"5.1\% improvement % real comment", r"5.1\% improvement "),
        (r"row\\% real comment", "row" + "\\" * 2),
        (r"row\\\% retained", r"row\\\% retained"),
        (r"row\\\\% real comment", "row" + "\\" * 4),
        ("a% hidden\r\nb\\% kept\r\nc% EOF", "a\r\nb\\% kept\r\nc"),
    ],
)
def test_comments_respect_escape_parity_and_eof(source, expected):
    assert _strip_tex_comments(source) == expected


def test_extraction_preserves_percentages_and_word_boundaries(tmp_path):
    source = (
        "\\begin{document}\n"
        r"The threshold increases from 5.1\% to 10.0\%, a useful gain. % hidden comment"
        "\n"
        r"Memory error falls to 4.11\% with the same decoder."
        "\n"
        r"First row\\Second row\\% comment after a line break"
        "\n"
        r"Third row\\\% literal percent after a line break."
        "\n\\end{document}% hidden EOF"
    )
    path = tmp_path / "source.tar.gz"
    data = source.encode("utf-8")
    with tarfile.open(path, "w:gz") as archive:
        member = tarfile.TarInfo("main.tex")
        member.size = len(data)
        archive.addfile(member, io.BytesIO(data))

    text = extract_tex_code_from_tar(str(path), "test")["all"]
    assert r"5.1\% to 10.0\%, a useful gain." in text
    assert r"4.11\% with the same decoder." in text
    assert "First row Second row " in text
    assert r"Third row \% literal percent" in text
    assert "hidden" not in text
    assert "comment after" not in text
