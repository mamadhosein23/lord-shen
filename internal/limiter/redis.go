package limiter

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

const redisLuaScript = `
local key = KEYS[1]
local limit = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local now = tonumber(ARGV[3])

local clearBefore = now - window
redis.call('ZREMRANGEBYSCORE', key, 0, clearBefore)

local currentRequests = redis.call('ZCARD', key)
if currentRequests < limit then
    redis.call('ZADD', key, now, now .. '-' .. math.random(100000))
    redis.call('EXPIRE', key, math.ceil(window))
    return {1, limit - currentRequests - 1, now + window}
else
    local oldest = redis.call('ZRANGE', key, 0, 0, 'WITHSCORES')
    local resetTime = tonumber(oldest[2]) + window
    return {0, 0, resetTime}
end
`

type RedisLimiter struct {
	client *redis.Client
	limit  int
	window time.Duration
	sha    string
}

func NewRedisLimiter(client *redis.Client, limit int, window time.Duration) *RedisLimiter {
	h := sha1.New()
	h.Write([]byte(redisLuaScript))
	sha := hex.EncodeToString(h.Sum(nil))

	return &RedisLimiter{
		client: client,
		limit:  limit,
		window: window,
		sha:    sha,
	}
}

func (r *RedisLimiter) Allow(ctx context.Context, key string) (Result, error) {
	now := time.Now().UnixNano() / int64(time.Millisecond)
	windowMs := r.window.Milliseconds()

	res, err := r.client.EvalSha(ctx, r.sha, []string{key}, r.limit, windowMs, now).Result()
	if err != nil {
		// Fallback or load script if SHA not found on Redis server
		res, err = r.client.Eval(ctx, redisLuaScript, []string{key}, r.limit, windowMs, now).Result()
		if err != nil {
			return Result{}, fmt.Errorf("redis eval failed: %w", err)
		}
	}

	arr, ok := res.([]interface{})
	if !ok || len(arr) < 3 {
		return Result{}, fmt.Errorf("unexpected redis response format")
	}

	allowed := arr[0].(int64) == 1
	remaining := int(arr[1].(int64))
	resetMs := arr[2].(int64)
	resetAt := time.Unix(0, resetMs*int64(time.Millisecond))

	var retryAfter time.Duration
	if !allowed {
		retryAfter = time.Until(resetAt)
		if retryAfter < 0 {
			retryAfter = 0
		}
	}

	return Result{
		Allowed:    allowed,
		Limit:      r.limit,
		Remaining:  remaining,
		ResetAt:    resetAt,
		RetryAfter: retryAfter,
	}, nil
}

func (r *RedisLimiter) Close() error {
	return r.client.Close()
}
