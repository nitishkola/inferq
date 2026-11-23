import asyncio
import random

from inferq_worker.backends import MockBackend, TransientError


def test_mock_batching_amortises_overhead():
    b = MockBackend(base_ms=150, per_item_ms=10)
    one_by_one = 8 * b.latency_s(1, 64)
    batched = b.latency_s(8, 64)
    assert batched < one_by_one / 4


def test_mock_returns_one_result_per_prompt():
    b = MockBackend(base_ms=1, per_item_ms=0, per_token_ms=0)
    out = asyncio.run(b.generate(["a", "bb", "ccc"], [4, 4, 4]))
    assert len(out) == 3 and all(isinstance(r, str) for r in out)


def test_mock_failure_injection():
    b = MockBackend(base_ms=0, per_item_ms=0, per_token_ms=0, fail_rate=1.0, rng=random.Random(1))
    out = asyncio.run(b.generate(["a", "b"], [1, 1]))
    assert all(isinstance(r, TransientError) for r in out)
