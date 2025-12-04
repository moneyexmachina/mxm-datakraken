from __future__ import annotations

import pytest
from mxm_config import MXMConfig, make_subconfig

from mxm.datakraken.config.config import (
    ensure_justetf_config,
    load_justetf_policy,
)


def _cfg_without_policy() -> MXMConfig:
    return make_subconfig(
        {
            "sources": {
                "justetf": {
                    "root": "/tmp/x",
                    "profile_index_dir": "/tmp/x/pi",
                    "profiles_dir": "/tmp/x/pf",
                    "parsed_dir": "/tmp/x/pa",
                    "logs_dir": "/tmp/x/lg",
                    "dataio": {
                        "paths": {
                            "root": "/tmp/x",
                            "db_path": "/tmp/x/db.sqlite",
                            "responses_dir": "/tmp/x/resp",
                        },
                        "adapters": {
                            "http": {
                                "enabled": True,
                                "alias": "justetf",
                                "user_agent": "t",
                                "default_timeout": 1.0,
                            }
                        },
                        "_reserved": {},
                    },
                    # NOTE: no 'policy' node
                }
            }
        }
    )


def test_ensure_config_raises_when_policy_missing() -> None:
    cfg = _cfg_without_policy()
    with pytest.raises((AttributeError, KeyError)):
        ensure_justetf_config(cfg)


def test_loader_raises_when_policy_missing() -> None:
    cfg = _cfg_without_policy()
    with pytest.raises((AttributeError, KeyError)):
        _ = load_justetf_policy(cfg)
