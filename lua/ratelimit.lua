-- Token-bucket rate limiter, one bucket per tenant.
-- KEYS[1]: bucket key
-- ARGV: capacity, refillPerSecond, cost
-- Returns: {allowed(1|0), retryAfterMs}
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)

local capacity, rate, cost = tonumber(ARGV[1]), tonumber(ARGV[2]), tonumber(ARGV[3])
local b = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens = tonumber(b[1]) or capacity
local ts = tonumber(b[2]) or now
tokens = math.min(capacity, tokens + (now - ts) / 1000 * rate)

local allowed, retryAfter = 0, 0
if tokens >= cost then
  tokens = tokens - cost
  allowed = 1
else
  retryAfter = math.ceil((cost - tokens) / rate * 1000)
end
redis.call('HSET', KEYS[1], 'tokens', tostring(tokens), 'ts', now)
redis.call('PEXPIRE', KEYS[1], math.ceil(capacity / rate * 1000) + 1000)
return {allowed, retryAfter}
