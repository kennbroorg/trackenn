"""Etherscan/BscScan free-tier rate limit (3 calls/sec TOTAL): verifies api_get throttles
calls globally so a single new-wallet investigation's 6 sequential calls (or any other
burst) never exceed the limit and trigger a live NOTOK "Max calls per sec rate limit
reached (3/sec)" (reproduced manually against the real API, 2026-10-24)."""
import time

import pytest

from core import api_client


@pytest.fixture(autouse=True)
def reset_call_history():
    # INFO: _call_times is process-global state (shared across every api_get call);
    # tests must not leak timestamps into each other.
    api_client._call_times.clear()
    yield
    api_client._call_times.clear()


def test_throttle_allows_calls_up_to_the_limit_immediately():
    start = time.monotonic()
    for _ in range(api_client._RATE_LIMIT_CALLS):
        api_client._throttle()
    elapsed = time.monotonic() - start

    assert elapsed < 0.1


def test_throttle_delays_calls_beyond_the_limit():
    start = time.monotonic()
    for _ in range(api_client._RATE_LIMIT_CALLS + 1):
        api_client._throttle()
    elapsed = time.monotonic() - start

    assert elapsed >= api_client._RATE_LIMIT_WINDOW * 0.9


def test_no_window_ever_exceeds_the_limit():
    # INFO: Simulates a burst of 9 calls (1.5x a full wallet investigation) and checks
    # every possible 1-second window contains at most RATE_LIMIT_CALLS timestamps.
    start = time.monotonic()
    times = []
    for _ in range(9):
        api_client._throttle()
        times.append(time.monotonic() - start)

    for t in times:
        window = [x for x in times if t - api_client._RATE_LIMIT_WINDOW < x <= t]
        assert len(window) <= api_client._RATE_LIMIT_CALLS, f"{len(window)} calls within 1s ending at {t}"


def test_throttle_is_global_across_concurrent_threads():
    # INFO: api_get_many style usage (concurrent threads) must still respect the single
    # process-wide budget, not one budget per thread.
    from concurrent.futures import ThreadPoolExecutor

    start = time.monotonic()
    with ThreadPoolExecutor(max_workers=6) as executor:
        futures = [executor.submit(api_client._throttle) for _ in range(6)]
        for future in futures:
            future.result()
    elapsed = time.monotonic() - start

    assert elapsed >= api_client._RATE_LIMIT_WINDOW * 0.9
