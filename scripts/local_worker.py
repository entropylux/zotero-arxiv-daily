"""Compose local config without Hydra's Python 3.14-incompatible CLI parser."""
import os
import sys
from run_local import prepare


def main():
    config = prepare()
    config.executor.max_paper_num = 20
    os.environ["TOKENIZERS_PARALLELISM"] = "false"
    from loguru import logger
    from zotero_arxiv_daily.executor import Executor
    logger.remove()
    logger.add(sys.stdout, level="INFO", colorize=False)
    Executor(config).run()


if __name__ == "__main__":
    main()
