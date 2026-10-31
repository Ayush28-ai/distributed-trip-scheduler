"""Exponential backoff with full jitter. Pure: no DB, no web."""
import random
from typing import Callable


def backoff_delay(attempt: int, base_s: float = 1.0, cap_s: float = 60.0,
                  rng: Callable[[], float] = random.random) -> float:
    """Delay before retry number `attempt` (1-based): uniform in [0, min(cap, base * 2^(attempt-1))]."""
    ceiling = min(cap_s, base_s * (2 ** max(0, attempt - 1)))
    return rng() * ceiling
