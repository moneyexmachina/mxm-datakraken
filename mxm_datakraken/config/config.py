"""
Config helpers for mxm-datakraken.

This module translates the app config into the minimal shape that mxm-dataio
expects for JustETF, and centralizes where we read the adapter alias from.
"""

from __future__ import annotations

from typing import Any


def _get(d: dict[str, Any], *keys: str, default: Any = None) -> Any:
    cur: Any = d
    for k in keys:
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur


def dataio_for_justetf(cfg: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    """
    Return (adapter_alias, dataio_cfg) for JustETF.

    Expected config (default.yaml):
      paths.sources.justetf.root
      paths.sources.justetf.dataio.{db_path,responses_dir}
      paths.sources.justetf.http.alias   # optional, defaults to "http"
    """
    root = _get(cfg, "paths", "sources", "justetf", "root")
    dio = _get(cfg, "paths", "sources", "justetf", "dataio", default={})
    alias = _get(cfg, "paths", "sources", "justetf", "http", "alias", default="http")

    if not isinstance(root, str) or not root:
        raise ValueError("config missing paths.sources.justetf.root")
    db_path = dio.get("db_path")
    responses_dir = dio.get("responses_dir")
    if not (isinstance(db_path, str) and isinstance(responses_dir, str)):
        raise ValueError(
            "config missing paths.sources.justetf.dataio.{db_path,responses_dir}"
        )

    dataio_cfg = {
        "paths": {
            "data_root": root,
            "db_path": db_path,
            "responses_dir": responses_dir,
        }
    }
    return str(alias), dataio_cfg
