-- Report a failed attempt. Retries with capped exponential backoff + jitter,
-- or moves the job to the dead-letter queue.
-- ARGV: id, fence, error, retryable(1|0), baseBackoffMs, maxBackoffMs, jitter(0..1)
-- Returns: 'RETRY' | 'DEAD' | 'STALE'
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)

local id, fence, err = ARGV[1], ARGV[2], ARGV[3]
local retryable = ARGV[4] == '1'
local base, cap, jitter = tonumber(ARGV[5]), tonumber(ARGV[6]), tonumber(ARGV[7])

local jk = 'iq:job:' .. id
local f = redis.call('HMGET', jk, 'status', 'fence', 'attempts', 'maxAttempts')
if f[1] ~= 'RUNNING' or f[2] ~= fence then return 'STALE' end
redis.call('ZREM', 'iq:inflight', id)

local attempts, maxAttempts = tonumber(f[3]), tonumber(f[4])
if retryable and attempts < maxAttempts then
  -- "Equal jitter": half fixed, half random, so retries don't stampede.
  local delay = math.min(cap, base * 2 ^ (attempts - 1))
  delay = math.floor(delay / 2 + delay / 2 * jitter)
  redis.call('HSET', jk, 'status', 'RETRY_WAIT', 'lastError', err, 'retryAt', now + delay)
  redis.call('ZADD', 'iq:delayed', now + delay, id)
  redis.call('HINCRBY', 'iq:stats', 'retried', 1)
  return 'RETRY'
end
redis.call('HSET', jk, 'status', 'DEAD', 'lastError', err, 'finishedAt', now)
redis.call('RPUSH', 'iq:dlq', id)
redis.call('HINCRBY', 'iq:stats', 'dead', 1)
return 'DEAD'
