from loguru import logger
from pyzotero import zotero
from omegaconf import DictConfig, ListConfig
from .utils import glob_match
from .retriever import get_retriever_cls
from .protocol import CorpusPaper
import random
from datetime import datetime
from .reranker import get_reranker_cls
from .construct_email import render_email
from .utils import send_email
from openai import OpenAI
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict
import math
from time import perf_counter
from .delivery import DeliveryState


def normalize_path_patterns(patterns: list[str] | ListConfig | None, config_key: str) -> list[str] | None:
    if patterns is None:
        return None

    if not isinstance(patterns, (list, ListConfig)):
        raise TypeError(
            f"config.zotero.{config_key} must be a list of glob patterns or null, "
            'for example ["2026/survey/**"]. Single strings are not supported.'
        )

    if any(not isinstance(pattern, str) for pattern in patterns):
        raise TypeError(f"config.zotero.{config_key} must contain only glob pattern strings.")

    return list(patterns)


class Executor:
    def __init__(self, config:DictConfig):
        self.config = config
        self.include_path_patterns = normalize_path_patterns(config.zotero.include_path, "include_path")
        self.ignore_path_patterns = normalize_path_patterns(config.zotero.ignore_path, "ignore_path")
        self.retrievers = {
            source: get_retriever_cls(source)(config) for source in config.executor.source
        }
        self.reranker = get_reranker_cls(config.executor.reranker)(config)
        self.openai_client = OpenAI(api_key=config.llm.api.key, base_url=config.llm.api.base_url)
        self.delivery = DeliveryState(config.executor.get('state_dir'))
    def fetch_zotero_corpus(self) -> list[CorpusPaper]:
        logger.info("Fetching zotero corpus")
        zot = zotero.Zotero(self.config.zotero.user_id, 'user', self.config.zotero.api_key)
        collections = zot.everything(zot.collections())
        collections = {c['key']:c for c in collections}
        corpus = zot.everything(zot.items(itemType='conferencePaper || journalArticle || preprint'))
        corpus = [c for c in corpus if c['data']['abstractNote'] != '']
        def get_collection_path(col_key:str) -> str:
            if p := collections[col_key]['data']['parentCollection']:
                return get_collection_path(p) + '/' + collections[col_key]['data']['name']
            else:
                return collections[col_key]['data']['name']
        for c in corpus:
            paths = [get_collection_path(col) for col in c['data']['collections']]
            c['paths'] = paths
        logger.info(f"Fetched {len(corpus)} zotero papers")
        return [CorpusPaper(
            title=c['data']['title'],
            abstract=c['data']['abstractNote'],
            added_date=datetime.strptime(c['data']['dateAdded'], '%Y-%m-%dT%H:%M:%SZ'),
            paths=c['paths']
        ) for c in corpus]
    
    def filter_corpus(self, corpus:list[CorpusPaper]) -> list[CorpusPaper]:
        if self.include_path_patterns:
            logger.info(f"Selecting zotero papers matching include_path: {self.include_path_patterns}")
            corpus = [
                c for c in corpus
                if any(
                    glob_match(path, pattern)
                    for path in c.paths
                    for pattern in self.include_path_patterns
                )
            ]
        if self.ignore_path_patterns:
            logger.info(f"Excluding zotero papers matching ignore_path: {self.ignore_path_patterns}")
            corpus = [
                c for c in corpus
                if not any(
                    glob_match(path, pattern)
                    for path in c.paths
                    for pattern in self.ignore_path_patterns
                )
            ]
        if self.include_path_patterns or self.ignore_path_patterns:
            samples = random.sample(corpus, min(5, len(corpus)))
            samples = '\n'.join([c.title + ' - ' + '\n'.join(c.paths) for c in samples])
            logger.info(f"Selected {len(corpus)} zotero papers:\n{samples}\n...")
        return corpus

    
    def run(self, *, dry_run=False):
        started = perf_counter()
        self.stage_timings = {}
        limit = self.config.executor.max_paper_num
        workers = self.config.executor.get('llm_workers', 2)
        if type(limit) is not int or limit < 1 or type(workers) is not int or not 1 <= workers <= 4:
            raise ValueError('Paper limit must be positive and llm_workers must be between 1 and 4')
        if not dry_run:
            self.delivery.check_pending()
        corpus = self.fetch_zotero_corpus()
        corpus = self.filter_corpus(corpus)
        if len(corpus) == 0:
            raise ValueError('No Zotero papers with usable abstracts; check collection selection')
        self.stage_timings['zotero'] = perf_counter() - started
        stage = perf_counter()
        all_papers = []
        for source, retriever in self.retrievers.items():
            logger.info(f"Retrieving {source} papers...")
            papers = retriever.retrieve_metadata()
            if len(papers) == 0:
                logger.info(f"No {source} papers found")
                continue
            logger.info(f"Retrieved {len(papers)} {source} papers")
            all_papers.extend(papers)
        logger.info(f"Total {len(all_papers)} papers retrieved from all sources")
        self.stage_timings['metadata'] = perf_counter() - stage
        candidates = self.delivery.unseen(all_papers)
        reranked_papers = []
        if candidates:
            logger.info("Reranking papers...")
            stage = perf_counter()
            ranked = self.reranker.rerank(candidates, corpus)
            reranked_papers = [p for p in ranked if p.score is not None and math.isfinite(p.score) and p.score > 0][:limit]
            self.stage_timings['ranking'] = perf_counter() - stage
            stage = perf_counter()
            logger.info(f'Generating abstract-based TLDRs for {len(reranked_papers)} selected papers')
            with ThreadPoolExecutor(max_workers=workers) as pool:
                list(pool.map(lambda p: p.generate_tldr(self.openai_client, self.config.llm), reranked_papers))
            self.stage_timings['summaries'] = perf_counter() - stage
        self.email_content = render_email(reranked_papers)
        self.report = {'mode': 'zotero_only', 'metadata_count': len(all_papers),
                       'unseen_count': len(candidates), 'corpus_count': len(corpus),
                       'selected': [asdict(p) for p in reranked_papers],
                       'delivery': 'preview' if dry_run else 'not_sent'}
        if not dry_run and (reranked_papers or self.config.executor.send_empty):
            logger.info('Sending email...')
            stage = perf_counter()
            self.delivery.send(reranked_papers, lambda: send_email(self.config, self.email_content))
            self.report['delivery'] = 'sent'
            self.stage_timings['email'] = perf_counter() - stage
            logger.info('Email sent successfully')
        elif not reranked_papers and not dry_run:
            self.report['delivery'] = 'no_new_papers'
            logger.info('No unseen relevant papers; no email sent')
        self.stage_timings['total'] = perf_counter() - started
        self.report['timings'] = self.stage_timings
        logger.info(f'Zotero-only digest: {len(reranked_papers)} papers; timings: {self.stage_timings}')
        return reranked_papers
