"""Lightweight retry decorator with exponential backoff and jitter.

Used by network-bound extractors (API, database, SAP) to ride out transient
failures without pulling in a heavyweight dependency.
"""

from __future__ import annotations

import functools
import logging
import random
import time
from typing import Callable, Tuple, Type, TypeVar

logger = logging.getLogger(__name__)

F = TypeVar("F", bound=Callable[..., object])


def with_retry(
    *,
    attempts: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 30.0,
    backoff: float = 2.0,
    jitter: float = 0.1,
    exceptions: Tuple[Type[BaseException], ...] = (Exception,),
) -> Callable[[F], F]:
    """Retry the wrapped callable on the given exceptions.

    Parameters
    ----------
    attempts:
        Total number of tries (``1`` disables retrying).
    base_delay:
        Delay before the first retry, in seconds.
    max_delay:
        Upper bound for any single backoff delay.
    backoff:
        Multiplier applied to the delay after each failed attempt.
    jitter:
        Fractional random jitter (0..1) added to each delay to avoid
        thundering-herd retries.
    exceptions:
        Exception types that trigger a retry. Anything else propagates.
    """
    if attempts < 1:
        raise ValueError("attempts must be >= 1")

    def decorator(func: F) -> F:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            delay = base_delay
            last_exc: BaseException | None = None
            for attempt in range(1, attempts + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as exc:  # noqa: PERF203 - retry loop
                    last_exc = exc
                    if attempt == attempts:
                        break
                    sleep_for = min(delay, max_delay)
                    sleep_for += sleep_for * jitter * random.random()
                    logger.warning(
                        "%s failed (attempt %d/%d): %s — retrying in %.1fs",
                        func.__name__,
                        attempt,
                        attempts,
                        exc,
                        sleep_for,
                    )
                    time.sleep(sleep_for)
                    delay *= backoff
            assert last_exc is not None
            raise last_exc

        return wrapper  # type: ignore[return-value]

    return decorator
