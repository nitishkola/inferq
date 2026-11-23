-- Find jobs whose lease expired (worker crashed or stalled) and retry or
-- dead-letter them, exactly like a reported transient failure.
-- ARGV: limit, baseBackoffMs, maxBackoffMs, jitter(0..1)
-- Returns: {reapedCount, deadCount}
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)

local limit = tonumber(ARGV[1])
local base, cap, jitter = tonumber(ARGV[2]), tonumber(ARGV[3]), tonumber(ARGV[4])

local expired = redis.call('ZRANGEBYSCORE', 'iq:inflight', '-inf', now, 'LIMIT', 0, limit)
local reaped, dead = 0, 0
for _, id in ipairs(expired) do
  redis.call('ZREM', 'iq:inflight', id)
  local jk = 'iq:job:' .. id
  local f = redis.call('HMGET', jk, 'status', 'attempts', 'maxAttempts', 'worker')
  if f[1] == 'RUNNING' then
    reaped = reaped + 1
    redis.call('HINCRBY', 'iq:stats', 'leaseExpired', 1)
    local err = 'lease expired on worker ' .. (f[4] or '?')
    local attempts, maxAttempts = tonumber(f[2]), tonumber(f[3])
    if attempts < maxAttempts then
      local delay = math.min(cap, base * 2 ^ (attempts - 1))
      delay = math.floor(delay / 2 + delay / 2 * jitter)
      redis.call('HSET', jk, 'status', 'RETRY_WAIT', 'lastError', err, 'retryAt', now + delay)
      redis.call('ZADD', 'iq:delayed', now + delay, id)
      redis.call('HINCRBY', 'iq:stats', 'retried', 1)
    else
      dead = dead + 1
      redis.call('HSET', jk, 'status', 'DEAD', 'lastError', err, 'finishedAt', now)
      redis.call('RPUSH', 'iq:dlq', id)
      redis.call('HINCRBY', 'iq:stats', 'dead', 1)
    end
  end
end
return {reaped, dead}
