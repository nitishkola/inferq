#!/usr/bin/env python3
"""Chaos test: SIGKILL workers in the middle of a run and prove no job is lost.

1. Submits N jobs, each with an Idempotency-Key, then re-submits some keys to check that
   retried submissions return the original job instead of creating duplicates.
2. While workers are busy, hard-kills some of them (SIGKILL: no graceful shutdown, leases
   are left behind exactly as in a real crash), then restarts them.
3. Waits for every job and checks: nothing lost, nothing dead-lettered, and the jobs that
   were on the killed workers were recovered by the lease reaper.

Run from the repo root with the stack up:  python3 chaos/chaos_test.py --jobs 300 --kill 2
"""
import argparse
import json
import random
import subprocess
import sys
import time
import urllib.error
import urllib.request
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
TERMINAL = {"SUCCEEDED", "DEAD", "CANCELLED"}


def http(method, url, body=None, headers=None):
    data = json.dumps(body).encode() if body is not None else None
    r = urllib.request.Request(url, data=data, method=method,
                               headers={"Content-Type": "application/json", **(headers or {})})
    try:
        with urllib.request.urlopen(r, timeout=10) as resp:
            raw = resp.read()
            return resp.status, json.loads(raw) if raw else None
    except urllib.error.HTTPError as e:
        return e.code, None
    except (urllib.error.URLError, TimeoutError, ConnectionError) as e:
        return 0, {"transport_error": repr(e)}


def compose(*args):
    return subprocess.run(["docker", "compose", *args], cwd=ROOT, check=True,
                          capture_output=True, text=True).stdout


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--url", default="http://localhost:8080")
    ap.add_argument("--jobs", type=int, default=300)
    ap.add_argument("--kill", type=int, default=1, help="workers to SIGKILL")
    ap.add_argument("--after", type=float, default=2.0, help="seconds after submitting to kill")
    ap.add_argument("--no-restart", action="store_true", help="leave killed workers down")
    ap.add_argument("--timeout", type=float, default=180)
    a = ap.parse_args()

    if http("GET", f"{a.url}/actuator/health")[0] != 200:
        sys.exit(f"API not healthy at {a.url}; run `docker compose up -d --build` first")

    run = uuid.uuid4().hex[:8]
    pool = ThreadPoolExecutor(16)

    def submit(i):
        # Retrying a POST is safe here *because* of the Idempotency-Key: if the first attempt
        # reached the server but the response was lost, the retry returns the same job (200).
        body = None
        for attempt in range(6):
            status, body = http("POST", f"{a.url}/v1/jobs",
                                {"prompt": f"chaos {run} #{i}", "maxTokens": 32},
                                {"X-Tenant-Id": f"chaos-{i % 10}", "Idempotency-Key": f"{run}-{i}"})
            if status in (200, 202):
                return i, status, body.get("id")
            if status not in (0, 429, 503):
                break
            time.sleep(0.2 * 2 ** attempt)
        return i, status, (body or {}).get("transport_error")

    print(f"run {run}: submitting {a.jobs} jobs")
    ids = {}
    for i, status, job_id in pool.map(submit, range(a.jobs)):
        if status not in (200, 202):
            sys.exit(f"submission {i} failed after retries: HTTP {status} {job_id or ''}")
        ids[i] = job_id

    replay = random.sample(range(a.jobs), min(20, a.jobs))
    dup_ok = all(status == 200 and job_id == ids[i] for i, status, job_id in pool.map(submit, replay))
    print(f"idempotent re-submits ({len(replay)}): {'same job ids returned' if dup_ok else 'MISMATCH'}")

    time.sleep(a.after)
    workers = compose("ps", "-q", "worker").split()
    victims = random.sample(workers, min(a.kill, len(workers)))
    for v in victims:
        subprocess.run(["docker", "kill", "--signal", "KILL", v], check=True, capture_output=True)
    killed_at = time.time() * 1000
    print(f"SIGKILLed {len(victims)} of {len(workers)} workers")
    if not a.no_restart:
        time.sleep(3)
        compose("start", "worker")
        print("restarted killed workers")

    pending, done = set(ids.values()), {}
    deadline = time.monotonic() + a.timeout
    while pending and time.monotonic() < deadline:
        for job_id, (status, job) in zip(list(pending), pool.map(
                lambda j: http("GET", f"{a.url}/v1/jobs/{j}"), list(pending))):
            if status == 200 and job and job["status"] in TERMINAL:
                done[job_id] = job
                pending.discard(job_id)
        if pending:
            print(f"  waiting on {len(pending)} job(s)...")
            time.sleep(1)

    succeeded = [j for j in done.values() if j["status"] == "SUCCEEDED"]
    dead = [j for j in done.values() if j["status"] == "DEAD"]
    recovered = [j for j in succeeded if j["attempts"] > 1]
    recovery_ms = [j["finishedAt"] - killed_at for j in recovered if j["finishedAt"] > killed_at]

    print(f"\nsucceeded {len(succeeded)}/{a.jobs}   dead {len(dead)}   lost {len(pending)}")
    print(f"recovered after worker crash (attempts > 1): {len(recovered)}")
    if recovery_ms:
        print(f"slowest recovery: {max(recovery_ms) / 1000:.1f}s after the kill")
    passed = not pending and not dead and dup_ok
    print("\nPASS: zero jobs lost" if passed else "\nFAIL")
    sys.exit(0 if passed else 1)


if __name__ == "__main__":
    main()
