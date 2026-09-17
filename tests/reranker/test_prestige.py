from types import SimpleNamespace

import pytest

from zotero_arxiv_daily.reranker.prestige import apply_prestige_bonus


def paper(score, affiliations=None, authors=None):
    return SimpleNamespace(score=score, affiliations=affiliations, authors=authors or [])


def test_small_bonus_does_not_stack_or_overcome_large_relevance_gap():
    high = paper(7)
    boosted = paper(6, ["Physics, Harvard University", "MIT"], ["John Preskill"])
    near = paper(6.05)
    config = {"bonus": 0.1, "institutions": ["Harvard University", "MIT"], "authors": ["John Preskill"]}
    ranked = apply_prestige_bonus([high, near, boosted], config)
    assert ranked == [high, boosted, near]
    assert boosted.score == pytest.approx(6.1)


def test_missing_affiliation_and_substring_do_not_match():
    papers = [paper(5, ["Smith Institute"]), paper(5, None, ["J. Preskill"])]
    apply_prestige_bonus(papers, {"bonus": 0.1, "institutions": ["MIT"], "authors": ["John Preskill"]})
    assert [p.score for p in papers] == [5, 5]


def test_unicode_and_punctuation_in_affiliation():
    p = paper(5, ["ETH Zürich, Switzerland"])
    apply_prestige_bonus([p], {"bonus": 0.1, "institutions": ["ETH Zurich"]})
    assert p.score == pytest.approx(5.1)


@pytest.mark.parametrize("bonus", [-1, 0.2, float("nan")])
def test_invalid_bonus(bonus):
    with pytest.raises(ValueError):
        apply_prestige_bonus([], {"bonus": bonus})
