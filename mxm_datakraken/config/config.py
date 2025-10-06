"""
Configuration loader for mxm-datakraken.

Requires env and profile to be set explicitly.
Defaults to env="dev", profile="default" for local development.
"""

import os

from mxm_config import load_config

ENV = os.getenv("MXM_ENV", "dev")
PROFILE = os.getenv("MXM_PROFILE", "default")

cfg = load_config("mxm-datakraken", env=ENV, profile=PROFILE)

# Optional shortcuts
paths = cfg.paths
params = cfg.parameters
