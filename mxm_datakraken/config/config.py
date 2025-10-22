"""
Configuration views for mxm-datakraken.

This module provides small, typed helpers that return **read-only** OmegaConf
views (slices) rooted at specific subtrees of the package configuration. Use
these to wire adapters and components without import-time loading or global
mutation.

The helpers wrap `mxm_config.make_view`, enforcing a read-only `DictConfig`
for each targeted subtree:

- `datakraken_view` → the whole `mxm_datakraken` subtree
- `justetf_http_view` → HTTP params for the JustETF source
- `justetf_paths_view` → filesystem locations for JustETF artifacts
- `justetf_dataio_view` → DataIO policy knobs for JustETF

Typical usage
-------------
    from mxm_config import load_config
    from mxm_datakraken.config import (
        datakraken_view,
        justetf_http_view,
        justetf_paths_view,
        justetf_dataio_view,
    )
    from omegaconf import OmegaConf

    cfg = load_config(package="mxm-datakraken", env="dev", profile="default")

    # Read-only views (safe to pass around; cannot be mutated)
    http_view  = justetf_http_view(cfg)
    paths_view = justetf_paths_view(cfg)
    dataio_view = justetf_dataio_view(cfg)

    # If an adapter needs plain dicts (mutable), convert explicitly:
    http_dict  = OmegaConf.to_container(http_view, resolve=True)   # type: ignore[assignment]
    paths_dict = OmegaConf.to_container(paths_view, resolve=True)
    dataio_dict = OmegaConf.to_container(dataio_view, resolve=True)

Design notes
------------
- Views are **read-only** by construction. Attempting to assign to attributes
  will raise an OmegaConf read-only error at runtime.
- All helpers accept `resolve: bool = True`. When `True`, interpolations are
  resolved at view creation; set to `False` if a downstream consumer needs to
  defer resolution.
- These helpers do **not** load configuration. Callers must obtain an
  `MXMConfig` via `mxm_config.load_config(...)` and then slice views from it.
- The targeted keys are defined in the repo YAMLs under the package subtree
  `mxm_datakraken.*`. Missing keys will propagate the underlying error from
  `make_view`.

Types
-----
Args:
    cfg: MXMConfig
        A fully loaded, package-composed configuration (usually from
        `mxm_config.load_config`).
    resolve: bool
        Whether to resolve interpolations within the returned view.

Returns:
    MXMConfig:
        A read-only `DictConfig` view rooted at the requested subtree.

Raises:
    KeyError:
        If the requested subtree path does not exist.
    omegaconf.errors.ReadonlyConfigError:
        If mutation is attempted on a returned view.
"""

from __future__ import annotations

from mxm_config import MXMConfig, make_view


def datakraken_view(cfg: MXMConfig, *, resolve: bool = True) -> MXMConfig:
    """Read-only view rooted at `mxm_datakraken`."""
    return make_view(cfg, "mxm_datakraken", resolve=resolve)


def justetf_http_view(cfg: MXMConfig, *, resolve: bool = True) -> MXMConfig:
    """HTTP params for JustETF."""
    return make_view(cfg, "mxm_datakraken.sources.justetf.http", resolve=resolve)


def justetf_paths_view(cfg: MXMConfig, *, resolve: bool = True) -> MXMConfig:
    """Filesystem locations for JustETF artifacts."""
    return make_view(cfg, "mxm_datakraken.paths.sources.justetf", resolve=resolve)


def justetf_dataio_view(cfg: MXMConfig, *, resolve: bool = True) -> MXMConfig:
    """DataIO policy knobs for JustETF."""
    return make_view(cfg, "mxm_datakraken.sources.justetf.dataio", resolve=resolve)
