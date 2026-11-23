-- Move a dead-lettered job back to its ready queue with a fresh attempt budget.
-- ARGV: id
-- Returns: 1 if redriven, 0 if the job is not in the DLQ
local id = ARGV[1]
local jk = 'iq:job:' .. id
local f = redis.call('HMGET', jk, 'status', 'model', 'rank')
if f[1] ~= 'DEAD' then return 0 end
redis.call('LREM', 'iq:dlq', 0, id)
redis.call('HSET', jk, 'status', 'QUEUED', 'attempts', 0)
redis.call('HDEL', jk, 'finishedAt')
redis.call('ZADD', 'iq:ready:' .. f[2], tonumber(f[3]), id)
redis.call('HINCRBY', 'iq:stats', 'redriven', 1)
return 1
