package tokenbucket

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
if tokens >= 1 then
    allowed = 1
end
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
