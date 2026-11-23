-- Mark a job succeeded. Rejected (returns 0) if the fence token is stale.
-- ARGV: id, fence, result, resultTtlSec
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)

local id, fence, result, ttl = ARGV[1], ARGV[2], ARGV[3], tonumber(ARGV[4])
local jk = 'iq:job:' .. id
local f = redis.call('HMGET', jk, 'status', 'fence')
if f[1] ~= 'RUNNING' or f[2] ~= fence then return 0 end

redis.call('HSET', jk, 'status', 'SUCCEEDED', 'result', result, 'finishedAt', now)
redis.call('ZREM', 'iq:inflight', id)
redis.call('HINCRBY', 'iq:stats', 'succeeded', 1)
if ttl > 0 then redis.call('EXPIRE', jk, ttl) end
return 1
