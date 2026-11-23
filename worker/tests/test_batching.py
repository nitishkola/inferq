import asyncio

from inferq_worker.batching import collect_batch


class FakeQueue:
    """Jobs 'arrive' on a schedule of (time_s, count)."""

    def __init__(self, arrivals):
        self.arrivals = sorted(arrivals)
        self.t0 = None
        self.next_id = 0
        self.taken = 0

    async def claim(self, n):
        loop = asyncio.get_running_loop()
        if self.t0 is None:
            self.t0 = loop.time()
        now = loop.time() - self.t0
        available = sum(c for t, c in self.arrivals if t <= now) - self.taken
        k = max(0, min(n, available))
        self.taken += k
        out = list(range(self.next_id, self.next_id + k))
        self.next_id += k
        return out


def run(coro):
    return asyncio.run(coro)


def test_empty_queue_returns_immediately():
    q = FakeQueue([])
    assert run(collect_batch(q.claim, 8, 0.05)) == []


def test_full_batch_does_not_wait():
    q = FakeQueue([(0, 20)])

    async def go():
        t = asyncio.get_running_loop().time()
        batch = await collect_batch(q.claim, 8, 1.0)
        return batch, asyncio.get_running_loop().time() - t

    batch, elapsed = run(go())
    assert len(batch) == 8
    assert elapsed < 0.05


def test_waits_to_fill_batch_from_late_arrivals():
    q = FakeQueue([(0, 1), (0.01, 3)])
    assert len(run(collect_batch(q.claim, 8, 0.05))) == 4


def test_wait_is_bounded():
    q = FakeQueue([(0, 1), (0.5, 7)])

    async def go():
        t = asyncio.get_running_loop().time()
        batch = await collect_batch(q.claim, 8, 0.03)
        return batch, asyncio.get_running_loop().time() - t

    batch, elapsed = run(go())
    assert len(batch) == 1
    assert elapsed < 0.1
