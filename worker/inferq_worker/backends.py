"""Inference backends. Each takes a batch of prompts and returns one result per prompt:
either the completion text or an exception (TransientError is retried, PermanentError is not)."""
import asyncio
import random
from typing import Protocol, Union


class TransientError(Exception):
    """Worth retrying: timeouts, 5xx, OOM, a dropped connection."""


class PermanentError(Exception):
    """Retrying won't help: bad input, unknown model."""


Result = Union[str, Exception]


class Backend(Protocol):
    async def generate(self, prompts: list[str], max_tokens: list[int]) -> list[Result]: ...


class MockBackend:
    """Simulates a GPU so the whole system runs on a laptop.

    One forward pass costs a fixed overhead plus a small per-item cost, which is why batching
    pays off on real GPUs: a batch of 8 costs far less than 8 separate calls.
    """

    def __init__(self, base_ms: float = 150, per_item_ms: float = 10, per_token_ms: float = 0.2,
                 fail_rate: float = 0.0, rng: random.Random | None = None):
        self.base_ms = base_ms
        self.per_item_ms = per_item_ms
        self.per_token_ms = per_token_ms
        self.fail_rate = fail_rate
        self.rng = rng or random.Random()

    def latency_s(self, batch_size: int, max_tokens: int) -> float:
        return (self.base_ms + self.per_item_ms * batch_size + self.per_token_ms * max_tokens) / 1000

    async def generate(self, prompts: list[str], max_tokens: list[int]) -> list[Result]:
        await asyncio.sleep(self.latency_s(len(prompts), max(max_tokens)))
        out: list[Result] = []
        for p in prompts:
            if self.rng.random() < self.fail_rate:
                out.append(TransientError("simulated CUDA error"))
            else:
                out.append(f"[mock completion: {len(p)} chars in]")
        return out


def _classify_http(status: int, body: str) -> Exception:
    if status == 429 or status >= 500:
        return TransientError(f"HTTP {status}")
    return PermanentError(f"HTTP {status}: {body[:200]}")


class OllamaBackend:
    """Ollama has no batch endpoint; send the batch concurrently and let Ollama's own
    scheduler run them in parallel (set OLLAMA_NUM_PARALLEL to the batch size)."""

    def __init__(self, url: str, model: str, timeout_s: float):
        import httpx

        self._httpx = httpx
        self._client = httpx.AsyncClient(base_url=url, timeout=timeout_s)
        self.model = model

    async def _one(self, prompt: str, max_tokens: int) -> Result:
        try:
            r = await self._client.post("/api/generate", json={
                "model": self.model, "prompt": prompt, "stream": False,
                "options": {"num_predict": max_tokens}})
        except self._httpx.TransportError as e:
            return TransientError(f"ollama unreachable: {e!r}")
        if r.status_code != 200:
            return _classify_http(r.status_code, r.text)
        return r.json().get("response", "")

    async def generate(self, prompts: list[str], max_tokens: list[int]) -> list[Result]:
        return list(await asyncio.gather(*(self._one(p, m) for p, m in zip(prompts, max_tokens))))


class VLLMBackend:
    """vLLM's OpenAI-compatible server accepts a list of prompts in one request: true batching.
    The batch shares one max_tokens (the largest requested)."""

    def __init__(self, url: str, model: str, timeout_s: float):
        import httpx

        self._httpx = httpx
        self._client = httpx.AsyncClient(base_url=url, timeout=timeout_s)
        self.model = model

    async def generate(self, prompts: list[str], max_tokens: list[int]) -> list[Result]:
        try:
            r = await self._client.post("/v1/completions", json={
                "model": self.model, "prompt": prompts, "max_tokens": max(max_tokens)})
        except self._httpx.TransportError as e:
            return [TransientError(f"vllm unreachable: {e!r}")] * len(prompts)
        if r.status_code != 200:
            return [_classify_http(r.status_code, r.text)] * len(prompts)
        out: list[Result] = [TransientError("missing choice")] * len(prompts)
        for choice in r.json().get("choices", []):
            out[choice["index"]] = choice["text"]
        return out


def make_backend(cfg) -> Backend:
    if cfg.backend == "mock":
        return MockBackend(cfg.mock_base_ms, cfg.mock_per_item_ms, cfg.mock_per_token_ms, cfg.fail_rate)
    if cfg.backend == "ollama":
        return OllamaBackend(cfg.backend_url, cfg.backend_model, cfg.backend_timeout_s)
    if cfg.backend == "vllm":
        return VLLMBackend(cfg.backend_url, cfg.backend_model, cfg.backend_timeout_s)
    raise ValueError(f"unknown BACKEND {cfg.backend!r} (use mock, ollama or vllm)")
