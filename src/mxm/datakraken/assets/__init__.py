from __future__ import annotations

from importlib.resources import files


def read_text(rel_path: str) -> str:
    """
    Read a packaged asset as text. Use posix-style relative paths from assets/ root.
    Example: read_text("etf_universe/v1/etf_universe_isins.txt")
    """
    return files(__package__).joinpath(rel_path).read_text(encoding="utf-8")


def read_lines(rel_path: str) -> list[str]:
    return [
        ln.strip()
        for ln in read_text(rel_path).splitlines()
        if ln.strip() and not ln.startswith("#")
    ]
