"""Tests for LocalReranker — requires sentence-transformers, marked slow."""

import pytest
import numpy as np
import sys
from types import SimpleNamespace

from zotero_arxiv_daily.reranker.local import LocalReranker


@pytest.mark.slow
def test_local_reranker(config):
    reranker = LocalReranker(config)
    score = reranker.get_similarity_score(["hello", "world"], ["ping"])
    assert score.shape == (2, 1)


@pytest.mark.parametrize("has_cuda, expected", [(True, "cuda"), (False, "cpu")])
def test_device_selection_and_single_embedding_batch(config, monkeypatch, has_cuda, expected):
    import torch
    calls = []
    monkeypatch.setattr(torch.cuda, "is_available", lambda: has_cuda)

    class Encoder:
        def __init__(self, model, **kwargs):
            assert kwargs["device"] == expected
        def encode(self, texts, **kwargs):
            calls.append(texts)
            return np.eye(len(texts), dtype=np.float32)
        def similarity(self, left, right):
            return torch.from_numpy(left @ right.T)

    monkeypatch.setitem(sys.modules, "sentence_transformers", SimpleNamespace(SentenceTransformer=Encoder))
    config.executor.debug = True
    config.reranker.local.device = "auto"
    score = LocalReranker(config).get_similarity_score(["one", "two"], ["reference"])
    assert calls == [["one", "two", "reference"]]
    assert score.shape == (2, 1)
