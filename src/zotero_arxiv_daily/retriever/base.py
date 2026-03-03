from abc import ABC, abstractmethod
from omegaconf import DictConfig
from ..protocol import Paper, RawPaperItem
from concurrent.futures import ProcessPoolExecutor
from typing import Type
from loguru import logger
class BaseRetriever(ABC):
    name: str
    def __init__(self, config:DictConfig):
        self.config = config
        self.retriever_config = getattr(config.source,self.name)

    @abstractmethod
    def _retrieve_raw_papers(self) -> list[RawPaperItem]:
        pass

    @abstractmethod
    def convert_to_paper(self, raw_paper:RawPaperItem) -> Paper | None:
        pass

    def retrieve_papers(self) -> list[Paper]:
        raw_papers = self._retrieve_raw_papers()
        papers = []
        logger.info(f"Processing {len(raw_papers)} papers...")
        
        # 引入 tqdm 以显示实时进度
        from tqdm import tqdm
        
        # 方案：摒弃容易死锁的 ProcessPoolExecutor，改用单线程 for 循环
        # 这样不仅能避免 OOM，还能精准捕获导致问题的“毒论文”
        for raw_paper in tqdm(raw_papers, desc="Processing"):
            try:
                p = self.convert_to_paper(raw_paper)
                if p is not None:
                    papers.append(p)
            except Exception as e:
                # 如果遇到导致崩溃的论文，会打印出错误，但不会阻断后续论文的处理
                paper_id = getattr(raw_paper, 'id', 'Unknown ID')
                logger.error(f"Error processing paper {paper_id}: {e}")
                
        return papers
    
registered_retrievers = {}

def register_retriever(name:str):
    def decorator(cls):
        registered_retrievers[name] = cls
        cls.name = name
        return cls
    return decorator

def get_retriever_cls(name:str) -> Type[BaseRetriever]:
    if name not in registered_retrievers:
        raise ValueError(f"Retriever {name} not found")
    return registered_retrievers[name]
