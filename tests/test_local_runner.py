"""Local scheduling must avoid duplicate mail and keep credentials out of logs."""
from datetime import datetime
import importlib.util
import json
from pathlib import Path
import sys

import pytest

spec = importlib.util.spec_from_file_location(
    "local_runner", Path(__file__).resolve().parents[1] / "scripts/run_local.py"
)
runner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runner)


@pytest.fixture
def local_run(tmp_path, monkeypatch):
    monkeypatch.setattr(runner, "ROOT", tmp_path)
    monkeypatch.setattr(runner, "prepare", lambda: None)
    monkeypatch.setattr(sys, "argv", ["run_local.py"])
    for key in ("ZOTERO_KEY", "SENDER_PASSWORD", "OPENAI_API_KEY"):
        monkeypatch.setenv(key, "test-secret-" + key)
    return tmp_path


@pytest.mark.parametrize("exit_code", [0, 1])
def test_run_records_exit_and_redacts(local_run, monkeypatch, exit_code):
    class Child:
        stdout = ["API failed: test-secret-OPENAI_API_KEY\n"]

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def wait(self):
            return exit_code

    def launch(command, **kwargs):
        assert "-S" in command
        assert Path(command[3]).name == "local_worker.py"
        assert kwargs["cwd"] == local_run
        return Child()

    monkeypatch.setattr(runner.subprocess, "Popen", launch)
    assert runner.main() == exit_code
    status = json.loads((local_run / ".local-state/status.json").read_text())
    assert status["exit_code"] == exit_code
    assert bool(status["success_date"]) == (exit_code == 0)
    log = Path(status["log"]).read_text()
    assert "[REDACTED]" in log
    assert "test-secret" not in log


def test_successful_day_is_not_sent_twice(local_run, monkeypatch):
    state = local_run / ".local-state"
    state.mkdir()
    (state / "status.json").write_text(json.dumps({"success_date": datetime.now().date().isoformat()}))
    monkeypatch.setattr(runner.subprocess, "Popen", lambda *a, **k: pytest.fail("Duplicate run"))
    assert runner.main() == 0


def test_check_never_launches_email_process(local_run, monkeypatch):
    monkeypatch.setattr(sys, "argv", ["run_local.py", "--check"])
    monkeypatch.setattr(runner.subprocess, "Popen", lambda *a, **k: pytest.fail("Unexpected execution"))
    assert runner.main() == 0


def test_missing_credentials_stop_before_execution(local_run, monkeypatch):
    def missing():
        raise ValueError("Fill .env.local first. Missing: ZOTERO_KEY")
    monkeypatch.setattr(runner, "prepare", missing)
    monkeypatch.setattr(runner.subprocess, "Popen", lambda *a, **k: pytest.fail("Unexpected execution"))
    assert runner.main() == 2


def test_preview_preserves_daily_success_marker(local_run, monkeypatch):
    state = local_run / ".local-state"
    state.mkdir()
    status = {"success_date": datetime.now().date().isoformat()}
    (state / "status.json").write_text(json.dumps(status))
    monkeypatch.setattr(sys, "argv", ["run_local.py", "--preview"])

    class Child:
        stdout = []
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def wait(self): return 0

    def launch(command, **kwargs):
        assert command[-1] == "--preview"
        return Child()

    monkeypatch.setattr(runner.subprocess, "Popen", launch)
    assert runner.main() == 0
    assert json.loads((state / "status.json").read_text()) == status
    assert json.loads((state / "preview-status.json").read_text())["success_date"] is None
