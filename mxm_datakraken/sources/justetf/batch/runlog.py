"""
Run logging utilities for justETF batch orchestration.

Layout (under `base_path`):
    profiles/
      runs/
        <run_id>/
          progress.jsonl          # one JSON object per line
          ok/
            <ISIN>.ok             # empty marker file
          err/
            <ISIN>.json           # structured error payload

Notes:
- `RunLog` is append-only and idempotent:
  re-logging the same event just appends another line.
- Timestamps are recorded in UTC ISO-8601 with 'Z' suffix.
- `run_id` defaults to a UTC timestamp if not provided.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Literal, Optional

Status = Literal["ok", "skip", "err"]


def _utc_now_iso() -> str:
    """Return current UTC time in ISO-8601, suffixed with 'Z' (no microseconds)."""
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def _default_run_id() -> str:
    """Deterministic, filesystem-safe default run id (UTC)."""
    # Example: 2025-10-30T07-59-12
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%S")


class RunLog:
    """
    Lightweight run logger for batch jobs.

    Usage:
        log = RunLog(base_path, run_id=None)
        log.log(isin="IE00...", status="ok", bucket="2025-10-30")
        log.mark_ok("IE00...")
        log.mark_err("IE00...", {"isin": "IE00...", "error": "boom"})
    """

    def __init__(self, base_path: Path, run_id: Optional[str] = None) -> None:
        self._base_path = base_path
        self._run_id = run_id or _default_run_id()

        # Ensure directories exist
        self.run_dir.mkdir(parents=True, exist_ok=True)
        self.ok_dir.mkdir(parents=True, exist_ok=True)
        self.err_dir.mkdir(parents=True, exist_ok=True)

        # Touch progress file to ensure it exists (useful for quick tail -f)
        self.progress_path.touch(exist_ok=True)

    # ---------- Public API ----------

    @property
    def run_id(self) -> str:
        """The identifier of this run (directory name under profiles/runs/)."""
        return self._run_id

    def log(
        self,
        *,
        isin: str,
        status: Status,
        bucket: Optional[str] = None,
        reason: Optional[str] = None,
        error: Optional[str] = None,
        extra: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Append a line to progress.jsonl.

        Args:
            isin: Security identifier being processed.
            status: One of "ok", "skip", "err".
            bucket: The as_of_bucket (if known).
            reason: Optional reason (e.g., "exists" for skip).
            error: Optional error message (for err).
            extra: Optional dict to include custom fields.
        """
        rec: Dict[str, Any] = {
            "time": _utc_now_iso(),
            "isin": isin,
            "status": status,
        }
        if bucket is not None:
            rec["bucket"] = bucket
        if reason is not None:
            rec["reason"] = reason
        if error is not None:
            rec["error"] = error
        if extra:
            # do not override standard fields
            for k, v in extra.items():
                if k not in rec:
                    rec[k] = v

        # Write one JSON object per line (UTF-8, no ASCII escaping)
        import json

        with self.progress_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")

    def mark_ok(self, isin: str) -> None:
        """
        Create an empty OK marker file for this ISIN.
        """
        (self.ok_dir / f"{isin}.ok").touch()

    def mark_err(self, isin: str, error_json: Dict[str, Any]) -> None:
        """
        Write a structured error payload for this ISIN.

        Args:
            isin: Security identifier.
            error_json: Arbitrary JSON-serializable dict describing the error.
        """
        import json

        path = self.err_dir / f"{isin}.json"
        path.write_text(
            json.dumps(error_json, ensure_ascii=False, indent=2), encoding="utf-8"
        )

    # ---------- Paths (properties) ----------

    @property
    def runs_root(self) -> Path:
        return self._base_path / "profiles" / "runs"

    @property
    def run_dir(self) -> Path:
        return self.runs_root / self._run_id

    @property
    def progress_path(self) -> Path:
        return self.run_dir / "progress.jsonl"

    @property
    def ok_dir(self) -> Path:
        return self.run_dir / "ok"

    @property
    def err_dir(self) -> Path:
        return self.run_dir / "err"
