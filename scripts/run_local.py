"""Windows local entry point; run with the user-selected Python interpreter."""
import argparse
from datetime import datetime
import json
import os
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
DEPS = ROOT / ".local-deps"
CUDA_DEPS = ROOT / ".cuda-deps"
REQUIRED = ("ZOTERO_ID", "ZOTERO_KEY", "SENDER", "RECEIVER", "SENDER_PASSWORD",
            "OPENAI_API_KEY", "OPENAI_API_BASE")


def prepare():
    # Keep unrelated globally installed ML packages out of this runtime.
    sys.path[:] = [p for p in sys.path if "site-packages" not in p.lower()]
    runtime_paths = [str(ROOT / "src"), str(CUDA_DEPS), str(DEPS)]
    sys.path[:0] = runtime_paths
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env.local", override=True)
    missing = [key for key in REQUIRED if not os.environ.get(key, "").strip()]
    if missing:
        raise ValueError("Fill .env.local first. Missing: " + ", ".join(missing))
    os.environ["PYTHONPATH"] = os.pathsep.join(runtime_paths)
    os.environ["PYTHONIOENCODING"] = "utf-8"
    os.environ["PYTHONUTF8"] = "1"
    os.environ["HF_HOME"] = str(ROOT / ".cache" / "huggingface")
    if os.environ.get("ARXIV_DAILY_PROXY"):
        os.environ["HTTP_PROXY"] = os.environ["ARXIV_DAILY_PROXY"]
        os.environ["HTTPS_PROXY"] = os.environ["ARXIV_DAILY_PROXY"]
    from hydra import compose, initialize_config_dir
    from omegaconf import OmegaConf
    with initialize_config_dir(config_dir=str(ROOT / "config"), version_base=None):
        config = compose(config_name="local")
        OmegaConf.to_container(config, resolve=True, throw_on_missing=True)
    from zotero_arxiv_daily.executor import Executor  # Verify runtime imports too.
    return config


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true", help="Validate config/imports without network or email")
    parser.add_argument("--force", action="store_true", help="Allow another run after today's success")
    parser.add_argument("--preview", action="store_true", help="Run the full pipeline without sending email")
    args = parser.parse_args()
    try:
        prepare()
    except Exception as exc:
        # Report missing variable names, never resolved config or credential values.
        if isinstance(exc, ValueError) and str(exc).startswith("Fill .env.local"):
            print(str(exc), file=sys.stderr)
        else:
            print(f"Local preflight failed ({type(exc).__name__}); check dependencies and .env.local.", file=sys.stderr)
        return 2
    if args.check:
        print("Local config and imports OK; max 20 papers. No network calls or email sent.")
        return 0

    from filelock import FileLock, Timeout
    state_dir = ROOT / ".local-state"
    state_dir.mkdir(exist_ok=True)
    status_path = state_dir / ("preview-status.json" if args.preview else "status.json")
    try:
        with FileLock(state_dir / "run.lock", timeout=0):
            today = datetime.now().date().isoformat()
            previous = json.loads(status_path.read_text()) if status_path.exists() else {}
            if previous.get("success_date") == today and not args.force and not args.preview:
                print("Already completed today; skipping duplicate email.")
                return 0
            log_dir = ROOT / "logs" / "local"
            log_dir.mkdir(parents=True, exist_ok=True)
            stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
            log_path = log_dir / f"{stamp}.log"
            secrets = [os.environ[key] for key in ("ZOTERO_KEY", "SENDER_PASSWORD", "OPENAI_API_KEY")]
            command = [sys.executable, "-S", "-u", str(ROOT / "scripts/local_worker.py")]
            if args.preview:
                command.append("--preview")
            started = datetime.now().isoformat()
            with log_path.open("w", encoding="utf-8") as log:
                log.write(f"Started: {started}\n")
                with subprocess.Popen(command, cwd=ROOT, stdout=subprocess.PIPE,
                                      stderr=subprocess.STDOUT, text=True, encoding="utf-8",
                                      errors="replace") as child:
                    for line in child.stdout:
                        for secret in secrets:
                            line = line.replace(secret, "[REDACTED]")
                        log.write(line)
                        log.flush()
                    code = child.wait()
                log.write(f"\nExit code: {code}\n")
            status = {"started": started, "finished": datetime.now().isoformat(),
                      "exit_code": code, "log": str(log_path),
                      "success_date": today if code == 0 and not args.preview else previous.get("success_date")}
            temporary = status_path.with_suffix(".tmp")
            temporary.write_text(json.dumps(status, indent=2), encoding="utf-8")
            temporary.replace(status_path)
            print(f"Finished with exit code {code}. Log: {log_path}")
            return code
    except Timeout:
        print("Another local run is active; skipping.")
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
