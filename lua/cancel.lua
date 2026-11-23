-- Cancel a job in any non-terminal state.
-- ARGV: id
-- Returns: previous status, or 'NOT_FOUND'. Terminal states are returned unchanged.
local id = ARGV[1]
local jk = 'iq:job:' .. id
local f = redis.call('HMGET', jk, 'status', 'model')
local status = f[1]
if not status then return 'NOT_FOUND' end
if status == 'QUEUED' then
  redis.call('ZREM', 'iq:ready:' .. f[2], id)
elseif status == 'RETRY_WAIT' then
  redis.call('ZREM', 'iq:delayed', id)
elseif status == 'RUNNING' then
  -- The worker's eventual complete/fail is rejected by the status check.
  redis.call('ZREM', 'iq:inflight', id)
else
  return status
end
redis.call('HSET', jk, 'status', 'CANCELLED')
redis.call('HINCRBY', 'iq:stats', 'cancelled', 1)
return status
