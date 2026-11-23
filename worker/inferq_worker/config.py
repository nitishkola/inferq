import os
import socket
from dataclasses import dataclass


def _env(name: str, default: str) -> str:
    # Empty counts as unset, so docker-compose can pass "${VAR:-}" through safely.
    return os.environ.get(name) or default


@dataclass(frozen=True)
class Config:
    redis_url: str
    lua_dir: str
    model: str
    worker_id: str
    backend: str  # mock | ollama | vllm
    backend_url: str
    backend_model: str  # model name the backend knows (defaults to `model`)
    backend_timeout_s: float
    max_batch: int
    max_wait_ms: int
    lease_ms: int
    heartbeat_ms: int
    result_ttl_s: int
    base_backoff_ms: int
    max_backoff_ms: int
    idle_min_s: float
    idle_max_s: float
    metrics_port: int
    mock_base_ms: float
    mock_per_item_ms: float
    mock_per_token_ms: float
    fail_rate: float

    @staticmethod
    def from_env() -> "Config":
        model = _env("MODEL", "mock-small")
        cfg = Config(
            redis_url=_env("REDIS_URL", "redis://localhost:6379"),
            lua_dir=_env("LUA_DIR", os.path.join(os.path.dirname(__file__), "..", "..", "lua")),
            model=model,
            worker_id=_env("WORKER_ID", f"{socket.gethostname()}-{os.getpid()}"),
            backend=_env("BACKEND", "mock"),
            backend_url=_env("BACKEND_URL", "http://localhost:11434"),
            backend_model=_env("BACKEND_MODEL", model),
            backend_timeout_s=float(_env("BACKEND_TIMEOUT_S", "120")),
            max_batch=int(_env("MAX_BATCH", "8")),
            max_wait_ms=int(_env("MAX_WAIT_MS", "20")),
            lease_ms=int(_env("LEASE_MS", "15000")),
            heartbeat_ms=int(_env("HEARTBEAT_MS", "5000")),
            result_ttl_s=int(_env("RESULT_TTL_S", "86400")),
            base_backoff_ms=int(_env("BASE_BACKOFF_MS", "500")),
            max_backoff_ms=int(_env("MAX_BACKOFF_MS", "30000")),
            idle_min_s=float(_env("IDLE_MIN_S", "0.01")),
            idle_max_s=float(_env("IDLE_MAX_S", "0.25")),
            metrics_port=int(_env("METRICS_PORT", "9100")),
            mock_base_ms=float(_env("MOCK_BASE_MS", "150")),
            mock_per_item_ms=float(_env("MOCK_PER_ITEM_MS", "10")),
            mock_per_token_ms=float(_env("MOCK_PER_TOKEN_MS", "0.2")),
            fail_rate=float(_env("FAIL_RATE", "0")),
        )
        if cfg.heartbeat_ms * 2 >= cfg.lease_ms:
            raise ValueError("HEARTBEAT_MS must be well under LEASE_MS (at most half)")
        if cfg.max_batch < 1:
            raise ValueError("MAX_BATCH must be >= 1")
        return cfg
