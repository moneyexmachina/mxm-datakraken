from __future__ import annotations

import json
import re
from pathlib import Path
from typing import List

from mxm_datakraken.sources.justetf.batch.runlog import RunLog

ISO_Z_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$"
)  # e.g., 2025-10-30T08:15:00Z
RUN_ID_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}$"
)  # e.g., 2025-10-30T08-15-00


def _read_progress_lines(path: Path) -> List[dict]:
    raw = path.read_text(encoding="utf-8").strip()
    return [json.loads(line) for line in raw.splitlines()] if raw else []


def test_initializes_layout_and_progress_file(tmp_path: Path) -> None:
    log = RunLog(tmp_path, run_id="testrun-001")

    # Directories
    assert log.runs_root == tmp_path / "profiles" / "runs"
    assert log.run_dir == tmp_path / "profiles" / "runs" / "testrun-001"
    assert log.ok_dir.exists()
    assert log.err_dir.exists()

    # progress.jsonl exists and is empty
    assert log.progress_path.exists()
    assert log.progress_path.read_text(encoding="utf-8") == ""


def test_default_run_id_format(tmp_path: Path) -> None:
    log = RunLog(tmp_path)  # no run_id provided
    assert RUN_ID_RE.match(log.run_id), f"unexpected run_id format: {log.run_id}"
    # directory created
    assert (tmp_path / "profiles" / "runs" / log.run_id).exists()


def test_log_appends_jsonl_and_preserves_unicode(tmp_path: Path) -> None:
    log = RunLog(tmp_path, run_id="append-test")

    log.log(isin="IE00AAA11111", status="ok", bucket="2025-10-30")
    log.log(isin="IE00BBB22222", status="skip", bucket="2025-10-30", reason="exists")
    # unicode payload
    log.log(isin="IE00CCC33333", status="err", error="boom 💥")

    rows = _read_progress_lines(log.progress_path)
    assert len(rows) == 3

    # basic keys present
    assert rows[0]["isin"] == "IE00AAA11111" and rows[0]["status"] == "ok"
    assert rows[1]["status"] == "skip" and rows[1]["reason"] == "exists"
    assert rows[2]["status"] == "err" and rows[2]["error"] == "boom 💥"

    # timestamps are ISO-8601 Z
    for r in rows:
        assert "time" in r and ISO_Z_RE.match(r["time"]), (
            f"bad timestamp: {r.get('time')}"
        )


def test_extra_does_not_override_standard_fields(tmp_path: Path) -> None:
    log = RunLog(tmp_path, run_id="extra-test")
    log.log(
        isin="IE00AAA11111",
        status="ok",
        bucket="2025-10-30",
        extra={"status": "fake", "custom": 42},
    )
    rows = _read_progress_lines(log.progress_path)
    assert len(rows) == 1
    rec = rows[0]
    # standard fields unchanged
    assert rec["status"] == "ok"
    assert rec["bucket"] == "2025-10-30"
    # extra retained
    assert rec["custom"] == 42
    # attempted override ignored
    assert rec.get("status") == "ok"


def test_mark_ok_creates_marker(tmp_path: Path) -> None:
    log = RunLog(tmp_path, run_id="ok-test")
    log.mark_ok("IE00AAA11111")
    assert (log.ok_dir / "IE00AAA11111.ok").exists()


def test_mark_err_writes_payload(tmp_path: Path) -> None:
    log = RunLog(tmp_path, run_id="err-test")
    payload = {"isin": "IE00BAD", "error": "boom", "kind": "network"}
    log.mark_err("IE00BAD", payload)

    err_files = list(log.err_dir.glob("*.json"))
    assert len(err_files) == 1
    data = json.loads(err_files[0].read_text(encoding="utf-8"))
    assert data == payload
