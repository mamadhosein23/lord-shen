package limiter

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisTokenBucketLimiter evaluates limits using atomic Redis Lua scripts.
type RedisTokenBucketLimiter struct {
	client *redis.Client
	script *redis.Script

	prefix        string
	capacity      int64
	ratePerSecond float64
	keyTTL        time.Duration
}

// Lua script to enforce atomic Token Bucket algorithm
// KEYS[1]: bucket state key
// ARGV[1]: capacity (max tokens)
// ARGV[2]: refill_rate_per_ms (tokens generated per millisecond)
// ARGV[3]: now_ms (unix milliseconds)
// ARGV[4]: requested tokens (N)
// ARGV[5]: ttl_ms (bucket idle TTL in milliseconds)
//
// Returns array:
// [1] allowed (0/1)
// [2] remaining (floor(tokens))
// [3] retry_after_ms
// [4] reset_after_ms (same as retry_after_ms; time until next request can be allowed)
const redisTokenBucketLua = `
local key = KEYS[1]

local capacity = tonumber(ARGV[1])
local refill_rate_per_ms = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local requested = tonumber(ARGV[4])
local ttl_ms = tonumber(ARGV[5])

if not capacity or capacity <= 0 then
  return {0, 0, 0, 0}
end
if not refill_rate_per_ms or refill_rate_per_ms <= 0 then
  return {0, 0, 0, 0}
end
if not requested or requested <= 0 then
  requested = 1
end

local state = redis.call("HMGET", key, "tokens", "last_updated")
local tokens = tonumber(state[1])
local last_updated = tonumber(state[2])

if not tokens or not last_updated then
  tokens = capacity
  last_updated = now
else
  local delta = now - last_updated
  if delta < 0 then delta = 0 end
  local generated = delta * refill_rate_per_ms
  tokens = math.min(capacity, tokens + generated)
  last_updated = now
end

local allowed = 0
local remaining = math.floor(tokens)
local retry_after_ms = 0

if tokens >= requested then
  allowed = 1
  tokens = tokens - requested
  remaining = math.floor(tokens)
else
  local missing = requested - tokens
  retry_after_ms = math.ceil(missing / refill_rate_per_ms)
  if retry_after_ms < 0 then retry_after_ms = 0 end
end

-- persist state (both allow/deny) for correctness under concurrency
redis.call("HMSET", key, "tokens", tokens, "last_updated", last_updated)

-- TTL in ms precision; clamp to at least 1 second if provided
if ttl_ms and ttl_ms > 0 then
  if ttl_ms < 1000 then ttl_ms = 1000 end
  redis.call("PEXPIRE", key, ttl_ms)
end

return {allowed, remaining, retry_after_ms, retry_after_ms}
`

// NewRedisTokenBucketLimiter creates a Redis-backed token bucket limiter.
// capacity: max burst
// ratePerSecond: sustained tokens/second
// keyTTL: idle TTL for bucket keys (PEXPIRE)
func NewRedisTokenBucketLimiter(client *redis.Client, prefix string, capacity int64, ratePerSecond float64, keyTTL time.Duration) (*RedisTokenBucketLimiter, error) {
	if client == nil {
		return nil, errors.New("redis client is nil")
	}
	if prefix == "" {
		prefix = "rl:"
	}
	if capacity <= 0 {
		return nil, fmt.Errorf("capacity must be > 0, got %d", capacity)
	}
	if ratePerSecond <= 0 {
		return nil, fmt.Errorf("ratePerSecond must be > 0, got %f", ratePerSecond)
	}
	if keyTTL <= 0 {
		keyTTL = 10 * time.Minute
	}

	// Optional sanity: keyTTL should not be smaller than full refill time.
	minRefill := time.Duration((float64(capacity)/ratePerSecond)*float64(time.Second))
	if keyTTL < minRefill {
		return nil, fmt.Errorf("keyTTL (%v) must be >= bucket full refill time (%v)", keyTTL, minRefill)
	}

	return &RedisTokenBucketLimiter{
		client:        client,
		script:        redis.NewScript(redisTokenBucketLua),
		prefix:        prefix,
		capacity:      capacity,
		ratePerSecond: ratePerSecond,
		keyTTL:        keyTTL,
	}, nil
}

func (r *RedisTokenBucketLimiter) Allow(ctx context.Context, key string) (Result, error) {
	return r.AllowN(ctx, key, 1)
}

func (r *RedisTokenBucketLimiter) AllowN(ctx context.Context, key string, n int) (Result, error) {
	if n <= 0 {
		return Result{}, errors.New("n must be > 0")
	}

	now := time.Now()
	nowMs := now.UnixMilli()
	refillRatePerMs := r.ratePerSecond / 1000.0
	ttlMs := r.keyTTL.Milliseconds()

	res, err := r.script.Run(
		ctx,
		r.client,
		[]string{r.prefix + key},
		r.capacity,
		refillRatePerMs,
		nowMs,
		int64(n),
		ttlMs,
	).Result()
	if err != nil {
		return Result{}, fmt.Errorf("redis limiter script failed: %w", err)
	}

	arr, ok := res.([]interface{})
	if !ok || len(arr) < 4 {
		return Result{}, fmt.Errorf("unexpected script output type/len: %T %#v", res, res)
	}

	allowed, err := toInt64(arr[0])
	if err != nil {
		return Result{}, fmt.Errorf("parse allowed: %w", err)
	}
	remaining, err := toInt64(arr[1])
	if err != nil {
		return Result{}, fmt.Errorf("parse remaining: %w", err)
	}
	retryMs, err := toInt64(arr[2])
	if err != nil {
		return Result{}, fmt.Errorf("parse retry_after_ms: %w", err)
	}

	retryAfter := time.Duration(retryMs) * time.Millisecond

	return Result{
		Allowed:    allowed == 1,
		Limit:      int(r.capacity),
		Remaining:  int(remaining),
		ResetAt:    now.Add(retryAfter),
		RetryAfter: retryAfter,
	}, nil
}

// Close exists for compatibility with limiter engines that need cleanup.
// For Redis limiter, the client lifecycle should be managed outside (main).
func (r *RedisTokenBucketLimiter) Close() error { return nil }

// toInt64 converts common Redis reply types into int64 safely (no panics).
func toInt64(v interface{}) (int64, error) {
	switch x := v.(type) {
	case int64:
		return x, nil
	case int:
		return int64(x), nil
	case string:
		return strconv.ParseInt(x, 10, 64)
	case []byte:
		return strconv.ParseInt(string(x), 10, 64)
	default:
		return 0, fmt.Errorf("unsupported type %T", v)
	}
}
