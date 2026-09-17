from types import SimpleNamespace

import numpy as np
import pytest

from zotero_arxiv_daily.reranker.base import BaseReranker


class FixedReranker(BaseReranker):
    def get_similarity_score(self, candidates, references):
        return self.matrix


def test_either_interest_can_promote_a_paper(config):
    config.reranker.interests = {"weight": 0.35, "topics": ["atoms", "QEC"]}
    ranker = FixedReranker(config)
    ranker.matrix = np.array([[0.6, 0.1, 0.1], [0.5, 0.9, 0.1], [0.5, 0.1, 0.9]])
    papers = [SimpleNamespace(abstract=name) for name in ["old", "atoms", "code"]]
    corpus = [SimpleNamespace(abstract="library", added_date=1)]
    result = ranker.rerank(papers, corpus)
    assert [p.abstract for p in result] == ["atoms", "code", "old"]
    assert [p.score for p in result] == pytest.approx([6.4, 6.4, 4.25])


def test_disabled_interests_preserve_corpus_score(config):
    config.reranker.interests = {"weight": 0, "topics": ["atoms"]}
    ranker = FixedReranker(config)
    ranker.matrix = np.array([[0.6]])
    result = ranker.rerank([SimpleNamespace(abstract="paper")],
                           [SimpleNamespace(abstract="library", added_date=1)])
    assert result[0].score == pytest.approx(6)


@pytest.mark.parametrize("weight", [-0.1, 1.1, float("nan")])
def test_invalid_weight_rejected(config, weight):
    config.reranker.interests.weight = weight
    with pytest.raises(ValueError, match="weight"):
        FixedReranker(config).rerank([], [])
