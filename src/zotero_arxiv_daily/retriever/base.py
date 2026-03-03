from abc import ABC, abstractmethod
from omegaconf import DictConfig
from ..protocol import Paper, RawPaperItem
from concurrent.futures import ThreadPoolExecutor, as_completed # 修改1：改为多线程
from typing import Type
from loguru import logger
from tqdm import tqdm # 修改2：引入进度条

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
        
        # 控制并发数，避免被目标 API 限流封禁，建议值 5 到 10
        workers = min(10, getattr(self.config.executor, 'max_workers', 5))
        logger.info(f"Processing {len(raw_papers)} papers using ThreadPool with {workers} workers...")
        
        # 修改3：使用 ThreadPoolExecutor 替代 ProcessPoolExecutor
        with ThreadPoolExecutor(max_workers=workers) as executor:
            # 提交任务到线程池
            future_to_paper = {executor.submit(self.convert_to_paper, raw): raw for raw in raw_papers}
            
            # 配合 tqdm 实现实时进度条，同时捕获单个任务的异常
            for future in tqdm(as_completed(future_to_paper), total=len(raw_papers), desc="Processing"):
                try:
                    p = future.result()
                    if p is not None:
                        papers.append(p)
                except Exception as e:
                    # 修改4：异常隔离。单篇失败仅打印日志，不让整个程序崩溃
                    raw = future_to_paper[future]
                    paper_id = getattr(raw, 'id', 'Unknown ID')
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
