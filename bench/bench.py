#!/usr/bin/env python3
"""Open-loop load test with end-to-end latency report. Standard library only.

Submits jobs at a fixed rate (open loop: the rate doesn't slow down when the system does,
which is how real traffic behaves), waits for them to finish, and reports throughput and
latency percentiles from the server's own timestamps, overall and per priority.

    python3 bench/bench.py --rate 50 --duration 60 --label batch8
"""
import argparse
import json
import random
import time
import urllib.error
import urllib.request
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor

TERMINAL = {"SUCCEEDED", "DEAD", "CANCELLED"}


def http(method, url, body=None, headers=None, timeout=10):
    data = json.dumps(body).encode() if body is not None else None
    r = urllib.request.Request(url, data=data, method=method,
                               headers={"Content-Type": "application/json", **(headers or {})})
    try:
        with urllib.request.urlopen(r, timeout=timeout) as resp:
            raw = resp.read()
            return resp.status, json.loads(raw) if raw else None
    except urllib.error.HTTPError as e:
        return e.code, None
    except (urllib.error.URLError, TimeoutError, ConnectionError):
        return 0, None


def pct(xs, p):
    if not xs:
        return None
    xs = sorted(xs)
    k = (len(xs) - 1) * p / 100
    lo, hi = int(k), min(int(k) + 1, len(xs) - 1)
    return xs[lo] + (xs[hi] - xs[lo]) * (k - lo)


def fmt(v):
    return "-" if v is None else f"{v:,.0f}"


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--url", default="http://localhost:8080")
    ap.add_argument("--rate", type=float, default=50, help="submissions per second")
    ap.add_argument("--duration", type=float, default=30, help="seconds of submitting")
    ap.add_argument("--tenants", type=int, default=10)
    ap.add_argument("--max-tokens", type=int, default=64)
    ap.add_argument("--high-share", type=float, default=0.1)
    ap.add_argument("--low-share", type=float, default=0.3)
    ap.add_argument("--timeout", type=float, default=300, help="max seconds to wait for completion")
    ap.add_argument("--label", default="run")
    ap.add_argument("--out", default="bench/results.jsonl", help="append a JSON summary here")
    a = ap.parse_args()

    def pick_priority():
        r = random.random()
        return "HIGH" if r < a.high_share else "LOW" if r < a.high_share + a.low_share else "NORMAL"

    def submit(i):
        prio = pick_priority()
        t0 = time.perf_counter()
        status, body = http("POST", f"{a.url}/v1/jobs",
                            {"prompt": f"bench prompt {i}: explain backpressure in one line",
                             "priority": prio, "maxTokens": a.max_tokens},
                            {"X-Tenant-Id": f"bench-{i % a.tenants}"})
        ms = (time.perf_counter() - t0) * 1000
        return status, (body or {}).get("id"), prio, ms

    pool = ThreadPoolExecutor(max_workers=128)
    futures = []
    start = time.monotonic()
    i = 0
    print(f"[{a.label}] submitting at {a.rate:g}/s for {a.duration:g}s ...")
    while True:
        due = start + i / a.rate
        if due - start >= a.duration:
            break
        now = time.monotonic()
        if due > now:
            time.sleep(due - now)
        futures.append(pool.submit(submit, i))
        i += 1

    statuses, submit_ms, accepted = Counter(), [], {}
    for f in futures:
        status, job_id, prio, ms = f.result()
        statuses[status] += 1
        submit_ms.append(ms)
        if status == 202 and job_id:
            accepted[job_id] = prio
    print(f"[{a.label}] submitted {i}: " + ", ".join(f"HTTP {k}: {v}" for k, v in sorted(statuses.items())))

    pending, done = set(accepted), {}
    deadline = time.monotonic() + a.timeout
    while pending and time.monotonic() < deadline:
        for job_id, (status, job) in zip(list(pending), pool.map(
                lambda j: http("GET", f"{a.url}/v1/jobs/{j}"), list(pending))):
            if status == 200 and job and job["status"] in TERMINAL:
                done[job_id] = job
                pending.discard(job_id)
        if pending:
            time.sleep(0.5)
    pool.shutdown()

    ok = [j for j in done.values() if j["status"] == "SUCCEEDED"]
    e2e = [j["totalLatencyMs"] for j in ok]
    wait = [j["queueWaitMs"] for j in ok]
    span_s = (max(j["finishedAt"] for j in ok) - min(j["createdAt"] for j in ok)) / 1000 if ok else 0
    by_prio = defaultdict(list)
    for jid, j in done.items():
        if j["status"] == "SUCCEEDED":
            by_prio[accepted[jid]].append(j["totalLatencyMs"])

    summary = {
        "label": a.label, "rate": a.rate, "duration": a.duration, "submitted": i,
        "accepted": len(accepted),
        "rejected_429": statuses.get(429, 0), "rejected_503": statuses.get(503, 0),
        "succeeded": len(ok),
        "dead": sum(1 for j in done.values() if j["status"] == "DEAD"),
        "unfinished": len(pending),
        "retried_jobs": sum(1 for j in ok if j["attempts"] > 1),
        "throughput_per_s": round(len(ok) / span_s, 1) if span_s else None,
        "submit_p99_ms": pct(submit_ms, 99),
        "wait_p50_ms": pct(wait, 50), "wait_p99_ms": pct(wait, 99),
        "e2e_p50_ms": pct(e2e, 50), "e2e_p95_ms": pct(e2e, 95), "e2e_p99_ms": pct(e2e, 99),
        "e2e_p50_by_priority_ms": {p: pct(v, 50) for p, v in sorted(by_prio.items())},
    }

    print(f"\n  succeeded {summary['succeeded']}/{summary['accepted']}   dead {summary['dead']}   "
          f"unfinished {summary['unfinished']}   retried {summary['retried_jobs']}")
    print(f"  throughput        {summary['throughput_per_s']} jobs/s")
    print(f"  submit API p99    {fmt(summary['submit_p99_ms'])} ms")
    print(f"  queue wait        p50 {fmt(summary['wait_p50_ms'])}   p99 {fmt(summary['wait_p99_ms'])} ms")
    print(f"  end-to-end        p50 {fmt(summary['e2e_p50_ms'])}   p95 {fmt(summary['e2e_p95_ms'])}   "
          f"p99 {fmt(summary['e2e_p99_ms'])} ms")
    print("  e2e p50 by prio   " + "   ".join(
        f"{p} {fmt(v)}" for p, v in summary["e2e_p50_by_priority_ms"].items()) + " ms")

    if a.out:
        with open(a.out, "a") as fh:
            fh.write(json.dumps(summary) + "\n")
        print(f"\n  summary appended to {a.out}")


if __name__ == "__main__":
    main()
