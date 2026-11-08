package limiter

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// Ensure RedisTokenBucketLimiter satisfies the CloserLimiter interface at compile time.
var _ CloserLimiter = (*RedisTokenBucketLimiter)(nil)

// RedisTokenBucketLimiter evaluates limits using atomic Redis Lua scripts.
type RedisTokenBucketLimiter struct {
	client *redis.Client
	script *redis.Script

	prefix        string
	capacity      int64
	ratePerSecond float64
	keyTTL        time.Duration
}

// Lua script to enforce atomic Token Bucket algorithm and return precise rate limit metadata.
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
// [3] retry_after_ms (0 if allowed, or exact ms to wait)
// [4] full_refill_ms (exact ms until bucket is 100% full)
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

-- Calculate milliseconds required for a full refill from current tokens
local missing_for_full = capacity - tokens
local full_refill_ms = 0
if missing_for_full > 0 and refill_rate_per_ms > 0 then
  full_refill_ms = math.ceil(missing_for_full / refill_rate_per_ms)
end

-- Persist state using HSET (HMSET is deprecated)
redis.call("HSET", key, "tokens", tokens, "last_updated", last_updated)

-- TTL in ms precision; clamp to at least 1 second if provided
if ttl_ms and ttl_ms > 0 then
  if ttl_ms < 1000 then ttl_ms = 1000 end
  redis.call("PEXPIRE", key, ttl_ms)
end

return {allowed, remaining, retry_after_ms, full_refill_ms}
`

// NewRedisTokenBucketLimiter creates a Redis-backed token bucket limiter.
func NewRedisTokenBucketLimiter(
	client *redis.Client,
	prefix string,
	capacity int64,
	ratePerSecond float64,
	keyTTL time.Duration,
) (*RedisTokenBucketLimiter, error) {
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

	minRefill := time.Duration((float64(capacity) / ratePerSecond) * float64(time.Second))
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

// Allow evaluates a single permit request for the given key.
func (r *RedisTokenBucketLimiter) Allow(ctx context.Context, key string) (Result, error) {
	return r.AllowN(ctx, key, 1)
}

// AllowN evaluates whether n units are permitted using atomic Lua script execution.
func (r *RedisTokenBucketLimiter) AllowN(ctx context.Context, key string, n int) (Result, error) {
	if n <= 0 {
		return Result{}, errors.New("n must be > 0")
	}
	if int64(n) > r.capacity {
		return Result{}, fmt.Errorf("n (%d) must be <= capacity (%d)", n, r.capacity)
	}

	now := time.Now()
	res, err := r.executeScript(ctx, key, int64(n), now)
	if err != nil {
		return Result{}, err
	}

	return res, nil
}

// Inspect queries current quota status without consuming tokens (passing n = 0 or using current state).
func (r *RedisTokenBucketLimiter) Inspect(ctx context.Context, key string) (Result, error) {
	now := time.Now()
	// To inspect without consuming, we pass requested = 0 (handled as inspection or minimal check)
	// Or we can invoke script with requested = 0 (which defaults or checks current status).
	// Let's pass n = 1 with a small adjustment or use script logic.
	// Since Lua checks tokens >= requested, passing n=1 when checking if allowed:
	// Better yet, let's execute script with n=1 but we can inspect state. 
	// Actually, standard Inspect checks if at least 1 token is available without decrementing.
	// We can write a lightweight read or execute script with n=0 (Lua handles n <= 0 as 1).
	// To truly inspect without consumption, let's read HMGET directly in Go:
	
	valMap, err := r.client.HMGet(ctx, r.prefix+key, "tokens", "last_updated").Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return Result{}, fmt.Errorf("redis inspect failed: %w", err)
	}

	tokens := float64(r.capacity)
	lastUpdated := now

	if len(valMap) >= 2 && valMap[0] != nil && valMap[1] != nil {
		if t, err := toFloat64(valMap[0]); err == nil {
			tokens = t
		}
		if lu, err := toInt64(valMap[1]); err == nil {
			lastUpdated = time.UnixMilli(lu)
		}
		
		// Apply local refill calculation for fresh inspect view
		delta := now.Sub(lastUpdated).Seconds()
		if delta > 0 {
			tokens = math.Min(float64(r.capacity), tokens+delta*r.ratePerSecond)
		}
	}

	missingForFull := float64(r.capacity) - tokens
	fullRefillMs := int64(0)
	if missingForFull > 0 && r.ratePerSecond > 0 {
		fullRefillMs = int64(math.Ceil((missingForFull / r.ratePerSecond) * 1000.0))
	}

	resetAt := now.Add(time.Duration(fullRefillMs) * time.Millisecond)
	allowed := tokens >= 1.0

	return Result{
		Allowed:    allowed,
		Limit:      int(r.capacity),
		Remaining:  int(math.Floor(tokens)),
		ResetAt:    resetAt,
		RetryAfter: 0,
	}, nil
}

// executeScript runs the Redis Lua script and formats the resulting metadata.
func (r *RedisTokenBucketLimiter) executeScript(ctx context.Context, key string, n int64, now time.Time) (Result, error) {
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
		n,
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
	fullRefillMs, err := toInt64(arr[3])
	if err != nil {
		return Result{}, fmt.Errorf("parse full_refill_ms: %w", err)
	}

	retryAfter := time.Duration(retryMs) * time.Millisecond
	resetAfter := time.Duration(fullRefillMs) * time.Millisecond

	resObj := Result{
		Allowed:    allowed == 1,
		Limit:      int(r.capacity),
		Remaining:  int(remaining),
		ResetAt:    now.Add(resetAfter),
		RetryAfter: retryAfter,
	}

	if allowed == 1 {
		resObj.RetryAfter = 0
	}

	return resObj, nil
}

// Close is a no-op for Redis client lifecycle management.
func (r *RedisTokenBucketLimiter) Close() error {
	return nil
}

// toInt64 converts common Redis reply types into int64 safely.
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

// toFloat64 converts common Redis reply types into float64 safely.
func toFloat64(v interface{}) (float64, error) {
	switch x := v.(type) {
	case float64:
		return x, nil
	case string:
		return strconv.ParseFloat(x, 64)
	case []byte:
		return strconv.ParseFloat(string(x), 64)
	default:
		return 0, fmt.Errorf("unsupported type %T", v)
	}
}
