package tokenbucket

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/redis/go-redis/v9"
)

type Decision struct {
	Allowed    bool
	Remaining  int64
	RetryAfter time.Duration
}

type Redis struct {
	client redis.UniversalClient
	prefix string
	script *redis.Script
}

const script = `
local capacity = tonumber(ARGV[1])
local rate = tonumber(ARGV[2])
local ttl_ms = tonumber(ARGV[3])

-- زمان را از خود Redis می‌گیریم تا ساعت سرورهای برنامه
-- در تصمیم‌گیری distributed اختلاف ایجاد نکند.
local clock = redis.call("TIME")
local now = tonumber(clock[1]) * 1000
          + math.floor(tonumber(clock[2]) / 1000)

local previous = redis.call(
    "HMGET", KEYS[1], "tokens", "timestamp"
)

local tokens = tonumber(previous[1]) or capacity
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
			"capacity must be between 1 and 1,000,000",
		)
	}

	if math.IsNaN(ratePerSecond) ||
		math.IsInf(ratePerSecond, 0) ||
		ratePerSecond < 0.001 ||
		ratePerSecond > 1_000_000 {
		return Decision{}, errors.New(
			"rate must be between 0.001 and 1,000,000",
		)
	}

	// TTL را محدود می‌کنیم تا کلیدهای خیلی کم‌مصرف
	// برای مدت غیرمنتظره در Redis باقی نمانند.
	const maxLifetimeSeconds = 7 * 24 * 60 * 60

	lifetimeSeconds := float64(capacity) / ratePerSecond
	if lifetimeSeconds > maxLifetimeSeconds {
		return Decision{}, errors.New(
			"capacity / rate must not exceed seven days",
		)
	}

	ttlMilliseconds := int64(
		math.Ceil(lifetimeSeconds*1000),
	) + 1000

	// IP خام را به‌عنوان کلید Redis ذخیره نمی‌کنیم.
	digest := sha256.Sum256([]byte(identity))
	key := l.prefix + hex.EncodeToString(digest[:])

	result, err := l.script.Run(
		ctx,
		l.client,
		[]string{key},
		capacity,
		ratePerSecond,
		ttlMilliseconds,
	).Int64Slice()
	if err != nil {
		return Decision{}, fmt.Errorf("execute token bucket: %w", err)
	}

	if len(result) != 3 {
		return Decision{}, fmt.Errorf(
			"unexpected token-bucket result length: %d",
			len(result),
		)
	}

	return Decision{
		Allowed:    result[0] == 1,
		Remaining:  result[1],
		RetryAfter: time.Duration(result[2]) * time.Millisecond,
	}, nil
}

