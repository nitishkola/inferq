import asyncio
import time
from typing import Awaitable, Callable, TypeVar

T = TypeVar("T")


async def collect_batch(claim: Callable[[int], Awaitable[list[T]]], max_batch: int,
                        max_wait_s: float, poll_s: float = 0.005,
                        clock: Callable[[], float] = time.monotonic) -> list[T]:
    """Dynamic batching: once at least one job is claimed, keep claiming for up to
    `max_wait_s` to fill the batch. Trades a few ms of latency for much higher GPU throughput.
    Under heavy load the batch fills instantly, so the wait only applies when traffic is light."""
    jobs = list(await claim(max_batch))
    if not jobs:
        return []
    deadline = clock() + max_wait_s
    while len(jobs) < max_batch:
        remaining = deadline - clock()
        if remaining <= 0:
            break
        await asyncio.sleep(min(poll_s, remaining))
        jobs.extend(await claim(max_batch - len(jobs)))
    return jobs
