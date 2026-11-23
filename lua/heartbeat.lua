-- Extend leases for jobs this worker still owns.
-- ARGV: workerId, leaseMs, id1, fence1, id2, fence2, ...
-- Returns: list of ids whose lease was extended (anything missing was lost:
-- cancelled, or reaped after a missed heartbeat).
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)

local worker, leaseMs = ARGV[1], tonumber(ARGV[2])
local kept = {}
for i = 3, #ARGV, 2 do
  local id, fence = ARGV[i], ARGV[i + 1]
  local f = redis.call('HMGET', 'iq:job:' .. id, 'status', 'fence', 'worker')
  if f[1] == 'RUNNING' and f[2] == fence and f[3] == worker then
    redis.call('ZADD', 'iq:inflight', 'XX', now + leaseMs, id)
    table.insert(kept, id)
  end
end
return kept
