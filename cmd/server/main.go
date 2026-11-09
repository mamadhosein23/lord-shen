package tokenbucket

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math"

-- زمان را از خود Redis 
local last = tonumber(previous[2]) or now

-- اگر ساعت Redis به عقب برگشت، توکن اضافه تولید نکن.
local effective_now = math.max(now, last)
local elapsed = effective_now - last

tokens = math.min(
    capacity,
    tokens + elapsed * rate / 1000
)

local allowed = 0

if tokens >= 1 then
    allowed = 1
    tokens = tokens - 1
end

redis.call(
    "HSET",
    KEYS[1],
    "tokens", tostring(tokens),
    "timestamp", effective_now
)
redis.call("PEXPIRE", KEYS[1], ttl_ms)

local retry_ms = 0

if allowed == 0 then
    retry_ms = math.ceil((1 - tokens) * 1000 / rate)
end

return {
    allowed,
    math.floor(tokens),
    retry_ms
}
`

func New(client redis.UniversalClient, prefix string) *Redis {
	return &Redis{
		client: client,
		prefix: prefix,
		script: redis.NewScript(script),
	}
}

func (l *Redis) Allow(
	ctx context.Context,
	identity string,
	capacity int64,
	ratePerSecond float64,
) (Decision, error) {
	if identity == "" {
		return Decision{}, errors.New("empty rate-limit identity")
	}

	if capacity <= 0 || capacity > 1_000_000 {
		return Decision{}, errors.New(
			"capacity must be between 
