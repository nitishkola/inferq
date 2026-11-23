#!/usr/bin/env bash
# Tests the queue's Lua scripts against a real Redis (>= 7).
# Usage: tests/lua/run_tests.sh            (starts a throwaway redis-server on port 6399)
#        REDIS_PORT=6379 NO_SPAWN=1 tests/lua/run_tests.sh   (use an existing server; it gets FLUSHALL'd)
set -uo pipefail
cd "$(dirname "$0")/../.."
PORT="${REDIS_PORT:-6399}"
if [[ -z "${NO_SPAWN:-}" ]]; then
  redis-server --port "$PORT" --save '' --appendonly no --daemonize yes >/dev/null
  trap 'redis-cli -p "$PORT" shutdown nosave >/dev/null 2>&1' EXIT
  sleep 0.3
fi
R() { redis-cli -p "$PORT" "$@"; }
ev() { local s=$1; shift; redis-cli -p "$PORT" EVAL "$(cat "lua/$s.lua")" 0 "$@"; }
PASS=0; FAIL=0
check() { # name expected actual
  if [[ "$2" == "$3" ]]; then PASS=$((PASS+1)); echo "  ok   $1"
  else FAIL=$((FAIL+1)); echo "  FAIL $1: expected [$2] got [$3]"; fi
}
# submit <id> <prio> [idem] [maxDepth] [penalty] [maxAttempts]
submit() { ev submit "$1" t1 m1 "$2" "prompt-$1" 16 "${6:-3}" "${3:-}" 60 "${4:-0}" "${5:-0}"; }
claim() { ev claim m1 "$1" "$2" "$3"; }   # worker count leaseMs
st() { R HGET "iq:job:$1" status; }

echo "submit + idempotency + backpressure"
R FLUSHALL >/dev/null
check "submit ok"            "$(printf 'OK\na')"        "$(submit a 1 key1)"
check "duplicate idem key"   "$(printf 'DUPLICATE\na')" "$(submit a2 1 key1)"
check "duplicate not queued" "1"                        "$(R ZCARD iq:ready:m1)"
check "queue full"           "FULL"                     "$(submit b 1 '' 1 | head -1)"

echo "priority ordering + aging"
R FLUSHALL >/dev/null
submit low 2 '' 0 120000 >/dev/null; submit norm 1 '' 0 30000 >/dev/null; submit high 0 '' 0 0 >/dev/null
check "HIGH, NORMAL, LOW order" "high norm low" "$(claim w1 3 10000 | awk 'NR%5==1' | xargs)"
R FLUSHALL >/dev/null
submit oldlow 2 '' 0 50 >/dev/null; sleep 0.12; submit newhigh 0 '' 0 0 >/dev/null
check "old LOW beats new HIGH (no starvation)" "oldlow newhigh" "$(claim w1 2 10000 | awk 'NR%5==1' | xargs)"

echo "claim + fenced completion"
R FLUSHALL >/dev/null
submit j 1 >/dev/null
check "claim returns id,fence,prompt,maxTokens,attempt" "j 1 prompt-j 16 1" "$(claim w1 1 10000 | xargs)"
check "status RUNNING"          "RUNNING" "$(st j)"
check "empty queue claims none" ""        "$(claim w1 1 10000)"
check "stale fence rejected"    "0"       "$(ev complete j 99 out 0)"
check "complete accepted"       "1"       "$(ev complete j 1 out 0)"
check "status SUCCEEDED"        "SUCCEEDED" "$(st j)"
check "inflight cleared"        "0"       "$(R ZCARD iq:inflight)"
check "double complete rejected" "0"      "$(ev complete j 1 out 0)"

echo "retry with backoff, then dead-letter, then redrive"
R FLUSHALL >/dev/null
submit r 1 '' 0 0 2 >/dev/null
claim w1 1 10000 >/dev/null
check "fail -> RETRY"          "RETRY"      "$(ev fail r 1 boom 1 50 1000 0.5)"
check "status RETRY_WAIT"      "RETRY_WAIT" "$(st r)"
check "not promoted early"     "0"          "$(ev promote 100)"
sleep 0.1
check "promoted after backoff" "1"          "$(ev promote 100)"
check "attempt 2, fence 2"     "2 2"        "$(claim w1 1 10000 | awk 'NR==5||NR==2' | xargs | awk '{print $2, $1}')"
check "fail at max -> DEAD"    "DEAD"       "$(ev fail r 2 boom 1 50 1000 0.5)"
check "in DLQ"                 "r"          "$(R LRANGE iq:dlq 0 -1)"
check "redrive"                "1"          "$(ev redrive r)"
check "redriven QUEUED"        "QUEUED"     "$(st r)"
check "DLQ empty"              "0"          "$(R LLEN iq:dlq)"
R FLUSHALL >/dev/null
submit p 1 >/dev/null; claim w1 1 10000 >/dev/null
check "non-retryable -> DEAD immediately" "DEAD" "$(ev fail p 1 bad-input 0 50 1000 0.5)"

echo "crash recovery: lease expiry + zombie fencing"
R FLUSHALL >/dev/null
submit c 1 >/dev/null
claim dead-worker 1 100 >/dev/null
sleep 0.2
check "reaper finds expired lease" "$(printf '1\n0')" "$(ev reap 100 10 10 0)"
check "status RETRY_WAIT"          "RETRY_WAIT"      "$(st c)"
sleep 0.05; ev promote 100 >/dev/null
claim w2 1 10000 >/dev/null
check "zombie complete rejected"   "0"     "$(ev complete c 1 late 0)"
check "zombie fail rejected"       "STALE" "$(ev fail c 1 late 1 10 10 0)"
check "new owner completes"        "1"     "$(ev complete c 2 ok 0)"

echo "heartbeats"
R FLUSHALL >/dev/null
submit h 1 >/dev/null
claim w1 1 200 >/dev/null
sleep 0.1
check "heartbeat extends lease"     "h" "$(ev heartbeat w1 1000 h 1)"
check "other worker can't heartbeat" "" "$(ev heartbeat w9 1000 h 1)"
sleep 0.2
check "extended lease not reaped" "$(printf '0\n0')" "$(ev reap 100 10 10 0)"

echo "cancel"
R FLUSHALL >/dev/null
submit q 1 >/dev/null
check "cancel queued"           "QUEUED"    "$(ev cancel q)"
check "removed from ready"      "0"         "$(R ZCARD iq:ready:m1)"
check "cancel terminal no-op"   "CANCELLED" "$(ev cancel q)"
submit x 1 >/dev/null; claim w1 1 10000 >/dev/null
check "cancel running"          "RUNNING"   "$(ev cancel x)"
check "worker result discarded" "0"         "$(ev complete x 1 out 0)"
check "cancel unknown"          "NOT_FOUND" "$(ev cancel nope)"

echo "rate limiter"
R FLUSHALL >/dev/null
rl() { redis-cli -p "$PORT" EVAL "$(cat lua/ratelimit.lua)" 1 iq:rl:t1 2 1 1 | head -1; }
check "token 1" "1" "$(rl)"; check "token 2" "1" "$(rl)"; check "bucket empty" "0" "$(rl)"
sleep 1.05
check "refilled" "1" "$(rl)"

echo; echo "$PASS passed, $FAIL failed"
[[ $FAIL -eq 0 ]]
