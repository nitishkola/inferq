"""Thin client over the shared Lua scripts. Every state change is one atomic script call."""
import random
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class ClaimedJob:
    id: str
    fence: str
    prompt: str
    max_tokens: int
    attempt: int


class JobQueue:
    def __init__(self, client, *, lua_dir: str, model: str, worker_id: str, lease_ms: int,
                 result_ttl_s: int, base_backoff_ms: int, max_backoff_ms: int):
        def load(name: str):
            return client.register_script((Path(lua_dir) / f"{name}.lua").read_text())

        self._claim = load("claim")
        self._heartbeat = load("heartbeat")
        self._complete = load("complete")
        self._fail = load("fail")
        self.model = model
        self.worker_id = worker_id
        self.lease_ms = lease_ms
        self.result_ttl_s = result_ttl_s
        self.base_backoff_ms = base_backoff_ms
        self.max_backoff_ms = max_backoff_ms

    async def claim(self, n: int) -> list[ClaimedJob]:
        raw = await self._claim(keys=[], args=[self.model, self.worker_id, n, self.lease_ms])
        return [
            ClaimedJob(id=raw[i], fence=raw[i + 1], prompt=raw[i + 2],
                       max_tokens=int(raw[i + 3]), attempt=int(raw[i + 4]))
            for i in range(0, len(raw), 5)
        ]

    async def heartbeat(self, jobs: list[ClaimedJob]) -> set[str]:
        args: list = [self.worker_id, self.lease_ms]
        for j in jobs:
            args += [j.id, j.fence]
        return set(await self._heartbeat(keys=[], args=args))

    async def complete(self, job: ClaimedJob, result: str) -> bool:
        return await self._complete(keys=[], args=[job.id, job.fence, result, self.result_ttl_s]) == 1

    async def fail(self, job: ClaimedJob, error: str, *, retryable: bool) -> str:
        return await self._fail(keys=[], args=[
            job.id, job.fence, error[:1000], 1 if retryable else 0,
            self.base_backoff_ms, self.max_backoff_ms, random.random()])
