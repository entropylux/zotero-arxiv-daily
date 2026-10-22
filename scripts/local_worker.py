"""Execute the unmodified upstream daily email pipeline."""
import os
import sys
from run_local import prepare


def main():
    config, executor_class = prepare()
    os.environ["TOKENIZERS_PARALLELISM"] = "false"
    from loguru import logger
    logger.remove()
    logger.add(sys.stdout, level="INFO", colorize=False)
    executor_class(config).run()


if __name__ == "__main__":
    main()
