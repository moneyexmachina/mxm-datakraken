"""
download_profiles_subset.py

Run a controlled batch download of JustETF profiles for a filtered subset of ETFs.

Workflow:
1. Resolve data paths via mxm-config.
2. Load subset profile index from `profile_index/subset_latest.json`.
3. Execute run_batch() on that subset (download, parse, persist, snapshot).
4. Log progress and output snapshot path.

This script should be used for limited, legally compliant subsets only.
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

from mxm_config import load_config

from mxm_datakraken.sources.justetf.batch import run_batch


def load_subset_index(base_path: Path) -> list[dict]:
    """Load subset_latest.json containing filtered ETF profile index entries."""
    subset_path = base_path / "profile_index" / "subset_latest.json"
    if not subset_path.exists():
        raise FileNotFoundError(f"Subset index not found: {subset_path}")
    subset = json.loads(subset_path.read_text(encoding="utf-8"))
    print(f"Loaded subset index with {len(subset)} entries.")
    return subset


def main(rate_seconds: float = 2.0, force: bool = False) -> None:
    """Run batch collection for the ETF subset."""
    # 1️⃣ Resolve config
    cfg = load_config("mxm-datakraken", env="dev", profile="default")
    base_path = Path(cfg.paths.sources.justetf.root)

    # 2️⃣ Load subset
    subset = load_subset_index(base_path)

    # 3️⃣ Prepare run id (timestamped)
    run_id = datetime.utcnow().strftime("%Y-%m-%dT%H-%M-%S")

    # 4️⃣ Run the batch
    snapshot_path = run_batch(
        base_path=base_path,
        index_entries=subset,
        rate_seconds=rate_seconds,
        force=force,
        run_id=run_id,
    )

    print(f"✅ Subset download completed.\nSnapshot saved to: {snapshot_path}")


if __name__ == "__main__":
    import argparse

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
    args = parser.parse_args()
    main(rate_seconds=args.rate, force=args.force)
