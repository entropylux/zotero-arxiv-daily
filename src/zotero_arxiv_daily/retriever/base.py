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
        logger.info(f"Processing {len(raw_papers)} papers using ThreadPool...")
        
        from concurrent.futures import ThreadPoolExecutor, as_completed
        from tqdm import tqdm
        
        # 使用 ThreadPoolExecutor（多线程）替代容易 OOM 的 ProcessPoolExecutor
        # max_workers 建议保持默认，通常在 5-10 左右，既能提速又不会被对方 API 封 IP
        with ThreadPoolExecutor(max_workers=self.config.executor.max_workers) as executor:
            # 提交所有任务到线程池
            future_to_paper = {executor.submit(self.convert_to_paper, raw): raw for raw in raw_papers}
            
            # as_completed 会在某个线程完成时立刻 yield，配合 tqdm 完美实现进度条
            for future in tqdm(as_completed(future_to_paper), total=len(raw_papers), desc="Processing"):
                try:
                    p = future.result()
                    if p is not None:
                        papers.append(p)
                except Exception as e:
                    # 抓出毒论文或网络超时的具体报错，不影响其他论文
                    logger.error(f"Error processing paper: {e}")
                    
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
