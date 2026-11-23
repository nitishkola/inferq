-- Move retry-delayed jobs whose backoff has elapsed back onto their ready queue.
-- They keep their original rank, so a retried job doesn't lose its place.
-- ARGV: limit
-- Returns: number promoted
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)

local due = redis.call('ZRANGEBYSCORE', 'iq:delayed', '-inf', now, 'LIMIT', 0, tonumber(ARGV[1]))
local n = 0
for _, id in ipairs(due) do
  redis.call('ZREM', 'iq:delayed', id)
  local jk = 'iq:job:' .. id
  local f = redis.call('HMGET', jk, 'status', 'model', 'rank')
  if f[1] == 'RETRY_WAIT' then
    redis.call('HSET', jk, 'status', 'QUEUED')
    redis.call('ZADD', 'iq:ready:' .. f[2], tonumber(f[3]), id)
    n = n + 1
  end
end
return n
