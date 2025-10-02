"""
mxm_datakraken.sources.justetf.batch

Batch orchestration for justETF data collection.

This module coordinates:
1) loading or refreshing the ETF Profile Index,
2) downloading any missing ETF profiles,
3) persisting them, and
4) building a daily snapshot.

Design notes:
- No implicit network calls for downstream read APIs. This module is the
  explicit "active" collection process.
- Idempotent by default: if a profile already exists and force=False, we skip it.
- Progress is logged to: profiles/runs/{run_id}/progress.jsonl with simple JSON lines.
- OK markers: profiles/runs/{run_id}/ok/{isin}.ok
- Error logs: profiles/runs/{run_id}/err/{isin}.json
"""

from __future__ import annotations

import json
import time
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Optional

from mxm_datakraken.sources.justetf.profile_index.api import get_profile_index
from mxm_datakraken.sources.justetf.profiles.downloader import download_etf_profile_html
from mxm_datakraken.sources.justetf.profiles.parser import (
    ETFProfileParsed,
    parse_profile,
)
from mxm_datakraken.sources.justetf.profiles.persistence import (
    save_profile,
    save_profiles_snapshot,
)


def run_batch(
    base_path: Path,
    rate_seconds: float = 2.0,
    force: bool = False,
    run_id: Optional[str] = None,
) -> Path:
    """
    Run a full batch collection for justETF profiles.

    Steps:
      - Load the latest Profile Index (no network if snapshot exists).
      - For each index entry, download/parse/save profile if missing (or if force=True).
      - Log per-ISIN progress (ok/skip/err) and markers.
      - Build a daily snapshot of profiles processed in this run.

    Args:
        base_path: Root data directory (contains profile_index/ and profiles/).
        rate_seconds: Delay between requests for politeness.
        force: If True, re-download even if a profile JSON already exists.
        run_id: Optional identifier for this run; defaults to current UTC timestamp.

    Returns:
        Path to the dated profiles snapshot JSON created for this run.
    """
    # 1) Load the Profile Index (no network if snapshot exists)
    index_entries = get_profile_index(base_path, force_refresh=False)

    # 2) Prepare run directories
    rid = run_id or datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%S")
    runs_dir = base_path / "profiles" / "runs"
    run_dir = runs_dir / rid
    run_dir.mkdir(parents=True, exist_ok=True)

    ok_dir = run_dir / "ok"
    ok_dir.mkdir(parents=True, exist_ok=True)

    err_dir = run_dir / "err"
    err_dir.mkdir(parents=True, exist_ok=True)

    progress_file = run_dir / "progress.jsonl"

    # Target directory for stored profiles (flat by ISIN)
    profiles_dir = base_path / "profiles"
    profiles_dir.mkdir(parents=True, exist_ok=True)

    # Profiles parsed during this run (used to build the snapshot)
    run_profiles: list[ETFProfileParsed] = []

    # 3) Process each index entry
    for entry in index_entries:
        isin = entry["isin"]
        url = entry["url"]

        target = profiles_dir / f"{isin}.json"
        if target.exists() and not force:
            with progress_file.open("a", encoding="utf-8") as f:
                f.write(json.dumps({"isin": isin, "status": "skip"}) + "\n")
            continue

        try:
            html = download_etf_profile_html(isin, url)
            parsed: ETFProfileParsed = parse_profile(html, isin)
            parsed["source_url"] = url  # provenance
            save_profile(parsed, profiles_dir)

            run_profiles.append(parsed)

            with progress_file.open("a", encoding="utf-8") as f:
                f.write(json.dumps({"isin": isin, "status": "ok"}) + "\n")
            (ok_dir / f"{isin}.ok").touch()

            time.sleep(rate_seconds)
        except Exception as exc:  # pragma: no cover
            with progress_file.open("a", encoding="utf-8") as f:
                f.write(
                    json.dumps(
                        {"isin": isin, "status": "err", "error": str(exc)},
                        ensure_ascii=False,
                    )
                    + "\n"
                )
            (err_dir / f"{isin}.json").write_text(
                json.dumps(
                    {"isin": isin, "error": str(exc)}, ensure_ascii=False, indent=2
                ),
                encoding="utf-8",
            )

    # 4) Build a dated snapshot from the profiles processed in this run
    snapshot_path = save_profiles_snapshot(
        run_profiles, base_path, as_of=date.today(), write_latest=True
    )
    return snapshot_path
