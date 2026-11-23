-- Enqueue a job atomically.
-- ARGV: id, tenant, model, priority(0..2), prompt, maxTokens, maxAttempts,
--       idemKey ('' = none), idemTtlSec, maxDepth (0 = unbounded), priorityPenaltyMs
-- Returns: {'OK', id} | {'DUPLICATE', existingId} | {'FULL', ''}
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)

local id, tenant, model, prio = ARGV[1], ARGV[2], ARGV[3], ARGV[4]
local prompt, maxTokens, maxAttempts = ARGV[5], ARGV[6], ARGV[7]
local idem, idemTtl = ARGV[8], tonumber(ARGV[9])
local maxDepth, penalty = tonumber(ARGV[10]), tonumber(ARGV[11])

local idemKey = nil
if idem ~= '' then
  idemKey = 'iq:idem:' .. tenant .. ':' .. idem
  local existing = redis.call('GET', idemKey)
  if existing then return {'DUPLICATE', existing} end
end

local readyKey = 'iq:ready:' .. model
if maxDepth > 0 and redis.call('ZCARD', readyKey) >= maxDepth then
  return {'FULL', ''}
end

if idemKey then redis.call('SET', idemKey, id, 'EX', idemTtl) end

-- Rank = enqueue time + priority penalty. Lower rank runs first. Because the
-- penalty is a fixed time offset, an old LOW job eventually outranks a new
-- HIGH job: priorities without starvation (aging for free).
local rank = now + penalty
redis.call('HSET', 'iq:job:' .. id,
  'id', id, 'tenant', tenant, 'model', model, 'priority', prio,
  'prompt', prompt, 'maxTokens', maxTokens,
  'attempts', 0, 'maxAttempts', maxAttempts, 'fence', 0,
  'status', 'QUEUED', 'createdAt', now, 'rank', rank)
redis.call('ZADD', readyKey, rank, id)
redis.call('SADD', 'iq:models', model)
redis.call('HINCRBY', 'iq:stats', 'submitted', 1)
return {'OK', id}
