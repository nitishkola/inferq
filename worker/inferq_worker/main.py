import asyncio
import logging
import signal
import time

import redis.asyncio as aioredis
from prometheus_client import Counter, Gauge, Histogram, start_http_server
from redis.exceptions import RedisError

from inferq_worker.backends import PermanentError, TransientError, make_backend
from inferq_worker.batching import collect_batch
from inferq_worker.config import Config
from inferq_worker.queue import ClaimedJob, JobQueue

log = logging.getLogger("inferq.worker")

JOBS = Counter("inferq_worker_jobs", "Job attempts finished by this worker", ["outcome"])
BATCH = Histogram("inferq_worker_batch_size", "Jobs per inference call",
                  buckets=(1, 2, 4, 8, 16, 32, 64))
INFER = Histogram("inferq_worker_inference_seconds", "Inference call latency",
                  buckets=(0.05, 0.1, 0.2, 0.5, 1, 2, 5, 10, 30, 60))
BUSY = Gauge("inferq_worker_busy", "1 while a batch is running")


class Worker:
    def __init__(self, cfg: Config, queue: JobQueue, backend):
        self.cfg = cfg
        self.queue = queue
        self.backend = backend
        self._stopping = asyncio.Event()

    def stop(self) -> None:
        if not self._stopping.is_set():
            log.info("shutdown requested: finishing current batch, claiming nothing new")
            self._stopping.set()

    async def run(self) -> None:
        idle = self.cfg.idle_min_s
        while not self._stopping.is_set():
            try:
                batch = await collect_batch(self.queue.claim, self.cfg.max_batch,
                                            self.cfg.max_wait_ms / 1000)
            except RedisError as e:
                log.warning("redis unavailable (%s), retrying in 1s", e)
                await asyncio.sleep(1)
                continue
            if not batch:
                # Exponential idle backoff keeps an empty queue from hammering Redis.
                try:
                    await asyncio.wait_for(self._stopping.wait(), timeout=idle)
                except asyncio.TimeoutError:
                    pass
                idle = min(idle * 2, self.cfg.idle_max_s)
                continue
            idle = self.cfg.idle_min_s
            await self.process(batch)
        log.info("drained, exiting")

    async def process(self, batch: list[ClaimedJob]) -> None:
        BATCH.observe(len(batch))
        BUSY.set(1)
        hb = asyncio.create_task(self._heartbeat(batch))
        t0 = time.perf_counter()
        try:
            results = await self.backend.generate([j.prompt for j in batch],
                                                  [j.max_tokens for j in batch])
            if len(results) != len(batch):
                raise RuntimeError(f"backend returned {len(results)} results for {len(batch)} prompts")
        except Exception as e:  # a crashed batch is a transient failure for every job in it
            log.exception("batch of %d failed", len(batch))
            results = [TransientError(repr(e))] * len(batch)
        finally:
            hb.cancel()
            BUSY.set(0)
        INFER.observe(time.perf_counter() - t0)

        for job, res in zip(batch, results):
            try:
                if isinstance(res, Exception):
                    outcome = await self.queue.fail(job, str(res),
                                                    retryable=not isinstance(res, PermanentError))
                    JOBS.labels(outcome.lower()).inc()  # retry | dead | stale
                elif await self.queue.complete(job, res):
                    JOBS.labels("succeeded").inc()
                else:
                    JOBS.labels("stale").inc()  # cancelled, or lease lost to another worker
            except RedisError as e:
                # The lease will expire and the reaper will retry it: at-least-once delivery.
                log.warning("could not report job %s: %s", job.id, e)

    async def _heartbeat(self, batch: list[ClaimedJob]) -> None:
        ids = {j.id for j in batch}
        while True:
            await asyncio.sleep(self.cfg.heartbeat_ms / 1000)
            try:
                kept = await self.queue.heartbeat(batch)
            except RedisError as e:
                log.warning("heartbeat failed: %s", e)
                continue
            if lost := ids - kept:
                log.warning("lost lease on %d job(s) (cancelled or reaped)", len(lost))


async def _amain(cfg: Config) -> None:
    client = aioredis.from_url(cfg.redis_url, decode_responses=True)
    queue = JobQueue(client, lua_dir=cfg.lua_dir, model=cfg.model, worker_id=cfg.worker_id,
                     lease_ms=cfg.lease_ms, result_ttl_s=cfg.result_ttl_s,
                     base_backoff_ms=cfg.base_backoff_ms, max_backoff_ms=cfg.max_backoff_ms)
    worker = Worker(cfg, queue, make_backend(cfg))
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, worker.stop)
    log.info("worker %s serving model %s via %s (max_batch=%d, max_wait=%dms)",
             cfg.worker_id, cfg.model, cfg.backend, cfg.max_batch, cfg.max_wait_ms)
    try:
        await worker.run()
    finally:
        await client.aclose()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    cfg = Config.from_env()
    start_http_server(cfg.metrics_port)
    asyncio.run(_amain(cfg))
