from __future__ import annotations

import time
from datetime import date
from pathlib import Path
from typing import Optional, Sequence, cast

from mxm_config import MXMConfig

from mxm_datakraken.sources.justetf.batch.core import (
    BatchStats,
    process_one_entry,
    resolve_bucket,
    should_skip,
)
from mxm_datakraken.sources.justetf.batch.runlog import RunLog
from mxm_datakraken.sources.justetf.common.models import (
    ETFProfileIndexEntry,
    JustETFProfile,
)
from mxm_datakraken.sources.justetf.profile_index.api import get_profile_index
from mxm_datakraken.sources.justetf.profiles.downloader import download_etf_profile_html
from mxm_datakraken.sources.justetf.profiles.parser import parse_profile
from mxm_datakraken.sources.justetf.profiles.persistence import (
    save_profile,
    save_profiles_snapshot,
)


def run_batch(
    cfg: MXMConfig,
    base_path: Path,
    index_entries: Optional[Sequence[ETFProfileIndexEntry]] = None,
    *,
    write_latest: bool = True,
    rate_seconds: float = 2.0,
    force_refresh: bool = False,
    run_id: Optional[str] = None,
) -> Path:
    """
    Thin orchestrator:
      1) load/receive index
      2) init run log
      3) loop entries: early skip → process_one_entry → log
      4) resolve bucket if still unknown
      5) write per-bucket snapshot
      6) return snapshot path
    """
    # 1) Load or use provided index
    entries: list[ETFProfileIndexEntry] = []
    if index_entries is None:
        entries = get_profile_index(cfg, base_path, force_refresh=False)

    # 2) Prepare run logging rid = run_id or datetime.now(timezone.utc)
    # .strftime("%Y-%m-%dT%H-%M-%S")
    log = RunLog(base_path=base_path, run_id=run_id)

    resolved_bucket: Optional[str] = None
    ok = skip = err = 0
    run_profiles: list[JustETFProfile] = []

    # 3) Process each index entry
    for entry in entries:
        isin = entry["isin"]

        # Early skip: only when bucket is known and not forcing
        do_skip, reason = should_skip(
            base_path=base_path,
            bucket=resolved_bucket,
            isin=isin,
            force_refresh=force_refresh,
        )
        if do_skip:
            log.log(isin=isin, status="skip", bucket=resolved_bucket, reason=reason)
            skip += 1
            continue

        # Download → parse → save (helper returns status + profile/bucket/error)
        status, profile, bucket_used, error = process_one_entry(
            cfg=cfg,
            base_path=base_path,
            entry=entry,
            bucket=resolved_bucket,
            download_html=download_etf_profile_html,
            parse=parse_profile,
            save=save_profile,  # persistence helper
            write_latest=write_latest,
        )

        if status == "ok":
            # Adopt first known bucket if not provided
            if resolved_bucket is None:
                resolved_bucket = cast(str, bucket_used)
            log.log(isin=isin, status="ok", bucket=resolved_bucket)
            log.mark_ok(isin)
            run_profiles.append(cast(JustETFProfile, profile))
            ok += 1
            time.sleep(rate_seconds)
        else:  # "err"
            log.log(isin=isin, status="err", bucket=resolved_bucket, error=error)
            log.mark_err(isin, {"isin": isin, "error": error})
            err += 1

    # 4) If still unknown (e.g., all skipped and no bucket passed), resolve now
    if resolved_bucket is None:
        resolved_bucket = resolve_bucket(
            provided=None,
            first_resp_bucket=None,
            profiles_root=base_path / "profiles",
            today_iso=date.today().isoformat(),
        )

    # 5) Write per-bucket aggregate snapshot for this run
    snapshot_path = save_profiles_snapshot(
        run_profiles,
        base_path=base_path,
        as_of_bucket=resolved_bucket,
        write_latest=write_latest,
    )

    # (Optional) build and return stats; we keep return type as Path for back-compat
    _ = BatchStats(
        bucket=resolved_bucket,
        ok=ok,
        skip=skip,
        err=err,
        snapshot_path=snapshot_path,
    )

    # 6) Return snapshot path for existing callers/tests
    return snapshot_path
