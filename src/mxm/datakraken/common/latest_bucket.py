"""
Helpers for maintaining and resolving a 'latest' pointer for bucketed artifacts.

We prefer a POSIX-style symlink named 'latest' that points to a bucket directory.
If creating symlinks fails (e.g., on filesystems without symlink support),
we fall back to a plain text marker file named 'LATEST_BUCKET' that contains
the bucket name.

Typical layout:
    <root>/
      <bucket-A>/
      <bucket-B>/
      latest -> <bucket-B>/
      LATEST_BUCKET   # only if symlink creation failed
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Optional


def update_latest_pointer(root: Path, bucket: str) -> None:
    """Update `<root>/latest` to point to the given `bucket`.

    This tries to create/replace a symlink:
        <root>/latest -> <bucket>/
    If symlinks are not supported or creation fails, writes a fallback file:
        <root>/LATEST_BUCKET  (containing the bucket name)

    Args:
        root: Directory that contains bucket subdirectories (and the 'latest' pointer).
        bucket: Name of the bucket directory to point at (relative to `root`).

    Raises:
        RuntimeError: If `<root>/latest` exists as a real directory (not a symlink).
    """
    latest = root / "latest"

    try:
        if latest.exists() or latest.is_symlink():
            # Don't remove a real directory accidentally
            if latest.is_dir() and not latest.is_symlink():
                raise RuntimeError(f"'latest' exists and is a real directory: {latest}")
            latest.unlink(missing_ok=True)

        # Use a relative symlink for portability
        latest.symlink_to(bucket)
    except OSError:
        """Fallback for filesystems that disallow symlinks or
        when permissions are missing."""
        (root / "LATEST_BUCKET").write_text(bucket, encoding="utf-8")


def resolve_latest_bucket(root: Path) -> Optional[str]:
    """Resolve the bucket name indicated by `<root>/latest` or `LATEST_BUCKET`.

    Resolution order:
      1) If `<root>/latest` is a symlink, return its (final path) name.
      2) Else, if `<root>/LATEST_BUCKET` exists, return its text (stripped).
      3) Else, return None.

    Args:
        root: Directory that contains the 'latest' symlink or fallback marker.

    Returns:
        The bucket name if resolvable, otherwise None.
    """
    latest = root / "latest"
    if latest.is_symlink():
        try:
            target = os.readlink(latest)
            # Normalize and return the terminal path component (the bucket name)
            return Path(target).name
        except OSError:
            # Broken symlink or unreadable; fall through to marker
            pass

    marker = root / "LATEST_BUCKET"
    if marker.exists():
        return marker.read_text(encoding="utf-8").strip() or None

    return None
