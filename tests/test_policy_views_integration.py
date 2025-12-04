from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Callable, cast

import pytest
from mxm_config import load_config
from mxm_dataio.api import CacheMode
from omegaconf import DictConfig
from omegaconf.errors import ReadonlyConfigError

from mxm.datakraken.config.config import (
    justetf_policy_view,
    load_justetf_policy,
)


def _load_cfg_from_repo_yaml(
    mxm_config_home: Callable[[str, str], Path],
    *,
    env: str = "dev",
    profile: str = "default",
) -> DictConfig:
    mxm_config_home("mxm-datakraken", "mxm.datakraken")
    cfg = cast(
        DictConfig, load_config(package="mxm-datakraken", env=env, profile=profile)
    )
    return cfg


def test_justetf_policy_view_readonly_and_has_keys(
    mxm_config_home: Callable[[str, str], Path],
) -> None:
    cfg = _load_cfg_from_repo_yaml(mxm_config_home)
    view = cast(DictConfig, justetf_policy_view(cfg))  # resolve=True by default

    # Core keys should be present (raw YAML values)
    for key in ("cache_mode", "ttl_seconds", "as_of_bucket"):
        assert key in view

    # Read-only enforcement
    with pytest.raises(ReadonlyConfigError):  # type: ignore[name-defined]
        view.cache_mode = "override"  # type: ignore[attr-defined]


def test_load_justetf_policy_resolves_runtime_values(
    mxm_config_home: Callable[[str, str], Path],
) -> None:
    cfg = _load_cfg_from_repo_yaml(mxm_config_home)
    policy = load_justetf_policy(cfg)

    assert isinstance(policy.cache_mode, CacheMode)
    # repo default.yaml uses ttl_seconds: 0 and as_of_bucket: "%Y-%m-%d"
    assert policy.ttl_seconds == 0.0
    assert policy.as_of_bucket == date.today().strftime("%Y-%m-%d")
