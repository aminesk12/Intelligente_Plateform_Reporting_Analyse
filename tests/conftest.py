"""Shared fixtures for the extractor test-suite.

The network-bound extractors (API, database, SAP) decorate methods with
``with_retry``. Real backoff sleeps would make the suite slow, so we patch the
retry module's ``time.sleep`` to a no-op for the whole session — retries still
happen, they just don't wait.
"""

from __future__ import annotations

import pandas as pd
import pytest


@pytest.fixture(autouse=True)
def _no_retry_sleep(monkeypatch):
    """Make retry backoff instantaneous across all tests."""
    import ETLS.core.retry as retry

    monkeypatch.setattr(retry.time, "sleep", lambda *_a, **_k: None)


@pytest.fixture
def sample_frame() -> pd.DataFrame:
    """A small, predictable DataFrame used to seed fixtures."""
    return pd.DataFrame(
        {
            "id": [1, 2, 3],
            "name": ["alice", "bob", "carol"],
            "amount": [10.5, 20.0, 30.25],
        }
    )
