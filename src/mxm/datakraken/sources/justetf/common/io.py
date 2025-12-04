from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator

from mxm_config import MXMConfig
from mxm_dataio.api import DataIoSession

from mxm.datakraken.config.config import dataio_for_justetf, load_justetf_policy


@contextmanager
def open_justetf_session(cfg: MXMConfig) -> Iterator[DataIoSession]:
    """
    Context manager that opens a DataIoSession for justETF with the
    configured CachePolicy (cache_mode, ttl, as_of_bucket).

    Usage:
        with open_justetf_session(cfg) as io:
            ...
    """
    source, dataio_cfg = dataio_for_justetf(cfg)
    policy = load_justetf_policy(cfg)

    with DataIoSession(
        source=source,
        cfg=dataio_cfg,
        cache_mode=policy.cache_mode,
        ttl=policy.ttl_seconds,
        as_of_bucket=policy.as_of_bucket,
    ) as io:
        yield io
