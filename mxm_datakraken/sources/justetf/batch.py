"""
mxm_datakraken.sources.justetf.batch

Batch orchestration for justETF data collection.

Coordinates:
1) loading or receiving the ETF Profile Index,
2) downloading any missing ETF profiles,
3) persisting them, and
4) building a daily snapshot.

Design notes:
- Idempotent by default: if a profile already exists and force=False, we skip it.
- Progress logged to: profiles/runs/{run_id}/progress.jsonl
- OK markers: profiles/runs/{run_id}/ok/{isin}.ok
- Error logs: profiles/runs/{run_id}/err/{isin}.json
"""

from __future__ import annotations

import json
import time
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Optional, Sequence

from mxm_datakraken.sources.justetf.profile_index.api import get_profile_index
from mxm_datakraken.sources.justetf.profiles.downloader import download_etf_profile_html
from mxm_datakraken.sources.justetf.profiles.model import JustETFProfile
from mxm_datakraken.sources.justetf.profiles.parser import parse_profile
from mxm_datakraken.sources.justetf.profiles.persistence import (
    save_profile,
    save_profiles_snapshot,
)


def run_batch(
    base_path: Path,
    index_entries: Sequence[dict] | None = None,
    rate_seconds: float = 2.0,
    force: bool = False,
    run_id: Optional[str] = None,
) -> Path:
    """
    Run a batch collection for justETF profiles.

    Args:
        base_path: Root data directory (contains profile_index/ and profiles/).
        index_entries: Optional sequence of ETFProfileIndexEntry dicts to process.
                       If None, the latest full index is loaded automatically.
        rate_seconds: Delay between requests for politeness.
        force: If True, re-download even if profile JSON already exists.
        run_id: Optional identifier for this run; defaults to current UTC timestamp.

    Returns:
        Path to the dated profiles snapshot JSON created for this run.
    """
    # 1) Load index entries
    if index_entries is None:
        index_entries = get_profile_index(base_path, force_refresh=False)

    print(f"Starting batch for {len(index_entries)} ETFs...")

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

    # Target directory for stored profiles
    profiles_dir = base_path / "profiles"
    profiles_dir.mkdir(parents=True, exist_ok=True)

    run_profiles: list[JustETFProfile] = []

    # 3) Process entries one by one
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
            parsed: JustETFProfile = parse_profile(html, isin)
            parsed["source_url"] = url
            save_profile(parsed, base_path)

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

    # 4) Build dated snapshot for profiles processed in this run
    snapshot_path = save_profiles_snapshot(
        run_profiles, base_path, as_of=date.today(), write_latest=True
    )

    print(
        f"✅ Batch completed. Processed {len(run_profiles)} profiles. "
        f"Snapshot saved to {snapshot_path}"
    )
    return snapshot_path
