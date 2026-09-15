"""Compose local config without Hydra's Python 3.14-incompatible CLI parser."""
import os
import sys
import json
from run_local import prepare, ROOT


def main():
    config = prepare()
    config.executor.max_paper_num = 20
    os.environ["TOKENIZERS_PARALLELISM"] = "false"
    from loguru import logger
    from zotero_arxiv_daily.executor import Executor
    logger.remove()
    logger.add(sys.stdout, level="INFO", colorize=False)
    executor = Executor(config)
    preview = "--preview" in sys.argv[1:]
    papers = executor.run(dry_run=preview)
    if preview:
        from zotero_arxiv_daily.construct_email import render_email
        state = ROOT / ".local-state"
        state.mkdir(exist_ok=True)
        (state / "preview.html").write_text(render_email(papers or []), encoding="utf-8")
        report = {
            "timings": executor.stage_timings,
            "papers": [{"url": p.url, "score": float(p.score), "fulltext_chars": len(p.full_text or ""),
                        "summary_present": bool(p.tldr), "summary_is_fallback": p.tldr == p.abstract,
                        "affiliations_present": p.affiliations is not None} for p in papers or []],
        }
        (state / "preview-metrics.json").write_text(json.dumps(report, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
