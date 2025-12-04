from __future__ import annotations

from typing import Iterable

from . import read_lines

DEFAULT_DATASET = "etf_universe/v1/etf_universe_isins.txt"


def load_default_isin_universe() -> list[str]:
    """Packaged default ISIN universe (stable, small, curated)."""
    return read_lines(DEFAULT_DATASET)


def load_isin_universe_from_text(text: str) -> list[str]:
    """Parse ISINs from a string (useful for overrides/tests)."""
    return [
        ln.strip() for ln in text.splitlines() if ln.strip() and not ln.startswith("#")
    ]


def load_isin_universe_override(paths: Iterable[str] | None) -> list[str] | None:
    """
    Optional override: first existing path wins.
    Keep this small and synchronous.
    """
    if not paths:
        return None
    from pathlib import Path

    for p in paths:
        path = Path(p)
        if path.is_file():
            return load_isin_universe_from_text(path.read_text(encoding="utf-8"))
    return None
