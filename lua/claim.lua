-- Worker claims up to N jobs for one model and takes a lease on each.
-- ARGV: model, workerId, count, leaseMs
-- Returns flat list: id, fence, prompt, maxTokens, attempt, (repeated per job)
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)

local model, worker = ARGV[1], ARGV[2]
local count, leaseMs = tonumber(ARGV[3]), tonumber(ARGV[4])

local popped = redis.call('ZPOPMIN', 'iq:ready:' .. model, count)
local out = {}
for i = 1, #popped, 2 do
  local id = popped[i]
  local jk = 'iq:job:' .. id
  if redis.call('HGET', jk, 'status') == 'QUEUED' then
    local attempt = redis.call('HINCRBY', jk, 'attempts', 1)
    -- Fencing token: bumps on every claim, so a "zombie" worker whose lease
    -- expired can never complete or fail a job that was handed to someone else.
    local fence = redis.call('HINCRBY', jk, 'fence', 1)
    redis.call('HSET', jk, 'status', 'RUNNING', 'worker', worker, 'startedAt', now)
    redis.call('ZADD', 'iq:inflight', now + leaseMs, id)
    local f = redis.call('HMGET', jk, 'prompt', 'maxTokens')
    table.insert(out, id)
    table.insert(out, tostring(fence))
    table.insert(out, f[1])
    table.insert(out, f[2])
    table.insert(out, tostring(attempt))
  end
end
return out
