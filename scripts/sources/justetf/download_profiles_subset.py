"""
download_profiles_subset.py

Run a controlled batch download of JustETF profiles for a filtered subset of ETFs.

Workflow:
1. Resolve data paths via mxm-config.
2. Ensure HTTP adapter is registered (bootstrap).
3. Load subset profile index from `profile_index/subset_latest.json`.
4. Execute run_batch() on that subset (download, parse, persist, snapshot).
5. Log progress and output snapshot path.

This script should be used for limited, legally compliant subsets only.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, List, Sequence, cast

from mxm_config import load_config

from mxm.datakraken.bootstrap import register_adapters_from_config
from mxm.datakraken.config.config import ensure_justetf_config
from mxm.datakraken.sources.justetf.batch import run_batch
from mxm.datakraken.sources.justetf.common.models import ETFProfileIndexEntry


def _validate_subset(items: Sequence[dict[str, Any]]) -> list[ETFProfileIndexEntry]:
    """Validate/normalize JSON-loaded items to ETFProfileIndexEntry list."""
    out: List[ETFProfileIndexEntry] = []
    for i, row in enumerate(items):
        isin = row.get("isin")
        url = row.get("url")
        lastmod = row.get("lastmod")
        if not isinstance(isin, str) or not isinstance(url, str):
            raise ValueError(f"subset entry #{i} missing 'isin' or 'url' (str)")
        if lastmod is not None and not isinstance(lastmod, str):
            raise ValueError(f"subset entry #{i} has non-str 'lastmod'")
        entry: ETFProfileIndexEntry = (
            {"isin": isin, "url": url, "lastmod": lastmod}
            if lastmod is not None
            else {"isin": isin, "url": url}
        )
        out.append(entry)
    return out


def load_subset_index(base_path: Path) -> list[ETFProfileIndexEntry]:
    """Load profile_index/subset_latest.json containing filtered ETF index entries."""
    subset_path = base_path / "profile_index" / "subset_latest.json"
    if not subset_path.exists():
        raise FileNotFoundError(f"Subset index not found: {subset_path}")
    loaded = json.loads(subset_path.read_text(encoding="utf-8"))
    if not isinstance(loaded, list):
        raise ValueError("subset_latest.json must contain a JSON array")
    subset = _validate_subset(cast(Sequence[dict[str, Any]], loaded))
    print(f"Loaded subset index with {len(subset)} entries.")
    return subset


def main(
    *,
    rate_seconds: float = 2.0,
    force: bool = False,
    env: str = "dev",
    profile: str = "default",
) -> None:
    """Run batch collection for the ETF subset."""
    # 1) Config + adapter bootstrap
    cfg = load_config("mxm-datakraken", env=env, profile=profile)
    ensure_justetf_config(cfg)
    register_adapters_from_config(cfg)

    # 2) Resolve base path directly from cfg (works with DictConfig)
    base_path = Path(cfg.sources.justetf.root)  # type: ignore[attr-defined]

    # 3) Load subset
    subset = load_subset_index(base_path)

    # 4) Run the batch on this subset
    run_id = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%S")
    snapshot_path = run_batch(
        cfg=cfg,
        base_path=base_path,
        index_entries=subset,
        rate_seconds=rate_seconds,
        force=force,
        run_id=run_id,
    )

    print(f"✅ Subset download completed.\nSnapshot saved to: {snapshot_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Download and parse JustETF profiles for a filtered subset."
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-download profiles even if JSON already exists.",
    )
    parser.add_argument(
        "--rate",
        type=float,
        default=2.0,
        help="Seconds to wait between requests (default: 2.0).",
    )
    parser.add_argument(
        "--env", default=None, help="mxm-config environment (default: dev)"
    )
    parser.add_argument(
        "--profile", default=None, help="mxm-config profile (default: default)"
    )
    args = parser.parse_args()

    # Ensure we never pass Optional[str] to load_config
    main(
        rate_seconds=float(args.rate),
        force=bool(args.force),
        env=args.env or "dev",
        profile=args.profile or "default",
    )
