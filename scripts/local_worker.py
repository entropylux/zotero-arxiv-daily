"""Run the local Zotero-only digest."""
import os
import sys
from run_local import prepare, ROOT


def main():
    config = prepare()
    os.environ["TOKENIZERS_PARALLELISM"] = "false"
    from loguru import logger
    from zotero_arxiv_daily.executor import Executor
    from zotero_arxiv_daily.delivery import write_json
    logger.remove()
    logger.add(sys.stdout, level="INFO", colorize=False)
    executor = Executor(config)
    preview = "--preview" in sys.argv[1:]
    executor.run(dry_run=preview)
    state = ROOT / ".local-state"
    write_json(state / ("preview-report.json" if preview else "report.json"), executor.report)
    if preview:
        (state / "preview.html").write_text(executor.email_content, encoding="utf-8")


if __name__ == "__main__":
    main()
