from abc import ABC, abstractmethod
from omegaconf import DictConfig
from ..protocol import Paper, CorpusPaper
import numpy as np
from typing import Type
class BaseReranker(ABC):
    def __init__(self, config:DictConfig):
        self.config = config

    def rerank(self, candidates:list[Paper], corpus:list[CorpusPaper]) -> list[Paper]:
        interests = self.config.reranker.get("interests", {}) if self.config is not None else {}
        topics = list(interests.get("topics", []))
        weight = float(interests.get("weight", 0.0))
        if not np.isfinite(weight) or not 0 <= weight <= 1:
            raise ValueError("Interest weight must be between 0 and 1")
        if any(not isinstance(topic, str) or not topic.strip() for topic in topics):
            raise ValueError("Interest topics must be non-empty strings")
        if not candidates:
            return []
        if not corpus and not (topics and weight > 0):
            raise ValueError("Ranking needs a Zotero corpus or enabled interest topics")
        if weight == 0:
            topics = []
        corpus = sorted(corpus,key=lambda x: x.added_date,reverse=True)
        time_decay_weight = 1 / (1 + np.log10(np.arange(len(corpus)) + 1))
        if corpus:
            time_decay_weight = time_decay_weight / time_decay_weight.sum()
        references = [c.abstract for c in corpus] + topics
        sim = self.get_similarity_score([c.abstract for c in candidates], references)
        assert sim.shape == (len(candidates), len(references))
        scores = (sim[:, :len(corpus)] * time_decay_weight).sum(axis=1)
        if topics:
            topic_scores = sim[:, len(corpus):].max(axis=1)
            effective_weight = weight if corpus else 1.0
            scores = (1 - effective_weight) * scores + effective_weight * topic_scores
        scores = scores * 10
        for s,c in zip(scores,candidates):
            c.score = s
        candidates = sorted(candidates,key=lambda x: x.score,reverse=True)
        return candidates
    
    @abstractmethod
    def get_similarity_score(self, s1:list[str], s2:list[str]) -> np.ndarray:
        raise NotImplementedError

registered_rerankers = {}

def register_reranker(name:str):
    def decorator(cls):
        registered_rerankers[name] = cls
        return cls
    return decorator

def get_reranker_cls(name:str) -> Type[BaseReranker]:
    if name not in registered_rerankers:
        raise ValueError(f"Reranker {name} not found")
    return registered_rerankers[name]
