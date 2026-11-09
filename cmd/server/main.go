package tokenbucket

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
if tokens >= 1 then
    allowed = 1
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
