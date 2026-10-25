package limiter

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisTokenBucketLimiter evaluates limits using atomic Redis Lua scripts.
type RedisTokenBucketLimiter struct {
	client *redis.Client
	script *redis.Script
}

// Lua script to enforce atomic Token Bucket algorithm
// KEYS[1]: Token bucket state key
// ARGV[1]: Max tokens capacity
// ARGV[2]: Refill rate per millisecond
// ARGV[3]: Current timestamp in unix milliseconds
// ARGV[4]: Requested tokens (usually 1)
const tokenBucketLua = `
local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local refill_rate = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local requested = tonumber(ARGV[4])

local state = redis.call("HMGET", key, "tokens", "last_updated")
local tokens = tonumber(state[1])
local last_updated = tonumber(state[2])

if not tokens then
    tokens = capacity
    last_updated = now
else
    local delta = math.max(0, now - last_updated)
    local generated = delta * refill_rate
    tokens = math.min(capacity, tokens + generated)
    last_updated = now
end

if tokens >= requested then
    tokens = tokens - requested
    redis.call("HMSET", key, "tokens", tokens, "last_updated", last_updated)
    -- Expire bucket if inactive for full refill duration + safety margin
    local ttl = math.ceil((capacity / refill_rate) / 1000) + 60
    redis.call("EXPIRE", key, ttl)
    return {1, math.floor(tokens), 0}
else
    local wait_time_ms = math.ceil((requested - tokens) / refill_rate)
    return {0, math.floor(tokens), wait_time_ms}
end
`

func NewRedisLimiter(client *redis.Client) *RedisTokenBucketLimiter {
	return &RedisTokenBucketLimiter{
		client: client,
		script: redis.NewScript(tokenBucketLua),
	}
}

// Allow checks if the request is permitted.
// capacity: max burst capacity.
// ratePerSecond: sustained allowed requests per second.
func (r *RedisTokenBucketLimiter) Allow(ctx context.Context, key string, capacity int64, ratePerSecond float64) (allowed bool, remaining int64, retryAfter time.Duration, err error) {
	nowMs := time.Now().UnixNano() / int64(time.Millisecond)
	refillRatePerMs := ratePerSecond / 1000.0

	res, err := r.script.Run(ctx, r.client, []string{"rl:" + key}, capacity, refillRatePerMs, nowMs, 1).Result()
	if err != nil {
		return false, 0, 0, fmt.Errorf("redis limiter execution failed: %w", err)
	}

	values, ok := res.([]interface{})
	if !ok || len(values) < 3 {
		return false, 0, 0, fmt.Errorf("unexpected script output")
	}

	isAllowed := values[0].(int64) == 1
	tokensLeft := values[1].(int64)
	waitMs := values[2].(int64)

	return isAllowed, tokensLeft, time.Duration(waitMs) * time.Millisecond, nil
}
