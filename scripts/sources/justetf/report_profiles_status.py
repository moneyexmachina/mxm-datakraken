"""
report_profiles_status.py

Summarize the progress and results of the latest JustETF profile download run.

Reads the latest run directory under:
  profiles/runs/<run_id>/
and prints completion statistics and optional error samples.

Usage:
    poetry run python scripts/sources/justetf/report_profiles_status.py
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Literal, Mapping, Sequence, TypedDict, cast

from mxm_config import load_config
from rich.console import Console
from rich.table import Table

from mxm_datakraken.config.config import justetf_view

console = Console()


class ProgressRecord(TypedDict, total=False):
    isin: str
    status: Literal["ok", "skip", "err", "unknown"]
    error: str


def get_latest_run_dir(runs_root: Path) -> Path:
    """Return the most recent run directory."""
    if not runs_root.exists():
        raise FileNotFoundError(f"No runs directory found: {runs_root}")

    run_dirs = [p for p in runs_root.iterdir() if p.is_dir()]
    if not run_dirs:
        raise FileNotFoundError(f"No run directories found under {runs_root}")

    latest = max(run_dirs, key=lambda p: p.name)
    return latest


def load_progress(run_dir: Path) -> list[ProgressRecord]:
    """Load JSON lines from progress.jsonl."""
    progress_file = run_dir / "progress.jsonl"
    if not progress_file.exists():
        raise FileNotFoundError(f"No progress file found in {run_dir}")

    out: list[ProgressRecord] = []
    for line in progress_file.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        obj = json.loads(line)
        if not isinstance(obj, dict):
            # Skip malformed lines
            continue
        rec: ProgressRecord = {
            "isin": cast(str, obj.get("isin", "")),
            "status": cast(
                Literal["ok", "skip", "err", "unknown"], obj.get("status", "unknown")
            ),
        }
        if "error" in obj and isinstance(obj["error"], str):
            rec["error"] = obj["error"]
        out.append(rec)
    return out


def summarize_progress(progress: Sequence[ProgressRecord]) -> dict[str, int]:
    """Count ok / skip / err occurrences."""
    counts: dict[str, int] = {"ok": 0, "skip": 0, "err": 0}
    for record in progress:
        status = record.get("status", "unknown")
        if status in counts:
            counts[status] += 1
    counts["total"] = sum(counts.values())
    return counts


def display_summary(
    run_dir: Path,
    counts: Mapping[str, int],
    progress: Sequence[ProgressRecord],
) -> None:
    """Render summary with rich."""
    console.rule(f"[bold cyan]MXM JustETF Run Report ({run_dir.name})")

    table = Table(title="Run Summary")
    table.add_column("Status", style="cyan", justify="right")
    table.add_column("Count", style="white", justify="right")

    for key in ("ok", "skip", "err", "total"):
        table.add_row(key.upper(), str(counts.get(key, 0)))

    console.print(table)

    if counts.get("err", 0) > 0:
        err_dir = run_dir / "err"
        console.print(f"\n[bold red]Errors detected:[/bold red] ({counts['err']})\n")
        err_files = list(err_dir.glob("*.json"))[:5]
        for f in err_files:
            try:
                data = json.loads(f.read_text(encoding="utf-8"))
                if isinstance(data, dict):
                    isin = cast(str, data.get("isin", f.stem))
                    err = cast(str, data.get("error", ""))
                    console.print(f"- [red]{isin}[/red]: {err}")
                else:
                    console.print(f"- [red]{f.name}[/red] (unexpected error format)")
            except Exception:
                console.print(f"- [red]{f.name}[/red] (unreadable error file)")
        more = counts["err"] - len(err_files)
        if more > 0:
            console.print(f"... and {more} more\n")
    else:
        console.print("\n[green]No errors detected.[/green]")

    console.rule()


def main() -> None:
    cfg = load_config("mxm-datakraken", env="dev", profile="default")

    # Use the view: robust to layout changes (sources.justetf.paths.root vs .root)
    base_path = Path(justetf_view(cfg).root)  # type: ignore[attr-defined]

    runs_root = base_path / "profiles" / "runs"
    latest_run = get_latest_run_dir(runs_root)
    progress = load_progress(latest_run)
    counts = summarize_progress(progress)
    display_summary(latest_run, counts, progress)


if __name__ == "__main__":
    main()
