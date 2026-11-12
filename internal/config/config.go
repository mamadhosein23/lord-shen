package config

import (
	"errors"
	"fmt"
	"math"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds the application configuration parameters for HTTP and Limiter engines.
type Config struct {
	Addr            string        // Server listener address (e.g., ":8080", "127.0.0.1:8080")
	RedisAddr       string        // Redis host:port
	Rate            float64       // Token generation rate per second
	Burst           int           // Maximum bucket capacity
	KeyTTL          time.Duration // Time-to-live duration before idle limiter keys expire
	ShutdownTimeout time.Duration // Graceful shutdown context timeout
}

// Load reads and validates configuration parameters from environment variables.
func Load() (*Config, error) {
	var errs []error

	rate, err := getEnvFloat("RATE", 5.0)
	if err != nil {
		errs = append(errs, fmt.Errorf("RATE: %w", err))
	}

	burst, err := getEnvInt("BURST", 20)
	if err != nil {
		errs = append(errs, fmt.Errorf("BURST: %w", err))
	}

	keyTTL, err := getEnvDuration("KEY_TTL", 10*time.Minute)
	if err != nil {
		errs = append(errs, fmt.Errorf("KEY_TTL: %w", err))
	}

	shutdownTimeout, err := getEnvDuration("SHUTDOWN_TIMEOUT", 5*time.Second)
	if err != nil {
		errs = append(errs, fmt.Errorf("SHUTDOWN_TIMEOUT: %w", err))
	}

	if len(errs) > 0 {
		return nil, errors.Join(errs...)
	}

	cfg := &Config{
		Addr:            getEnv("ADDR", ":8080"),
		RedisAddr:       getEnv("REDIS_ADDR", "localhost:6379"),
		Rate:            rate,
		Burst:           burst,
		KeyTTL:          keyTTL,
		ShutdownTimeout: shutdownTimeout,
	}

	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("config validation failed: %w", err)
	}

	return cfg, nil
}

// validate ensures all configuration parameters fall within safe production limits.
func (c *Config) validate() error {
	var errs []error

	// 1. Validate Rate (prevent <= 0, NaN, and Inf)
	if c.Rate <= 0 || math.IsNaN(c.Rate) || math.IsInf(c.Rate, 0) {
		errs = append(errs, fmt.Errorf("rate must be a positive finite number, got %f", c.Rate))
	}

	// 2. Validate Burst
	if c.Burst <= 0 {
		errs = append(errs, fmt.Errorf("burst must be > 0, got %d", c.Burst))
	}

	// 3. KeyTTL must be positive on its own, not only through the refill check
	if c.KeyTTL <= 0 {
		errs = append(errs, fmt.Errorf("key_ttl must be positive, got %v", c.KeyTTL))
	}

	// 4. Token bucket consistency check: TTL must be >= full refill time
	// Skipped when an earlier check failed, since the refill math needs a valid rate and burst
	if len(errs) == 0 {
		refillSec := float64(c.Burst) / c.Rate
		if refillSec > float64(math.MaxInt64/int64(time.Second)) {
			errs = append(errs, errors.New("burst/rate ratio creates duration overflow"))
		} else {
			minRefillDuration := time.Duration(refillSec * float64(time.Second))
			if c.KeyTTL < minRefillDuration {
				errs = append(errs, fmt.Errorf("key_ttl (%v) must be >= bucket refill time (%v)", c.KeyTTL, minRefillDuration))
			}
		}
	}

	// 5. Validate Timeouts
	if c.ShutdownTimeout <= 0 {
		errs = append(errs, errors.New("shutdown_timeout must be positive"))
	}

	// 6. Validate Network Addresses
	if err := validateAddr(c.Addr, true); err != nil {
		errs = append(errs, fmt.Errorf("invalid listener addr %q: %w", c.Addr, err))
	}

	if err := validateAddr(c.RedisAddr, false); err != nil {
		errs = append(errs, fmt.Errorf("invalid redis addr %q: %w", c.RedisAddr, err))
	}

	return errors.Join(errs...)
}

// validateAddr verifies host:port or :port specifications correctly.
func validateAddr(addr string, allowPortOnly bool) error {
	target := addr
	if allowPortOnly && !strings.Contains(target, ":") {
		target = ":" + target
	}
	_, port, err := net.SplitHostPort(target)
	if err != nil {
		return err
	}
	if port == "" {
		return errors.New("missing port in address")
	}
	p, err := strconv.Atoi(port)
	if err != nil || p < 1 || p > 65535 {
		return fmt.Errorf("invalid port %q: must be between 1 and 65535", port)
	}
	return nil
}

// Helper functions for reading environment variables cleanly

func lookupEnvTrim(key string) (string, bool) {
	val, ok := os.LookupEnv(key)
	if !ok {
		return "", false
	}
	val = strings.TrimSpace(val)
	if val == "" {
		return "", false
	}
	return val, true
}

func getEnv(key, def string) string {
	if val, ok := lookupEnvTrim(key); ok {
		return val
	}
	return def
}

func getEnvInt(key string, def int) (int, error) {
	val, ok := lookupEnvTrim(key)
	if !ok {
		return def, nil
	}
	res, err := strconv.Atoi(val)
	if err != nil {
		return 0, fmt.Errorf("invalid int: %w", err)
	}
	return res, nil
}

func getEnvFloat(key string, def float64) (float64, error) {
	val, ok := lookupEnvTrim(key)
	if !ok {
		return def, nil
	}
	res, err := strconv.ParseFloat(val, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid float: %w", err)
	}
	return res, nil
}

func getEnvDuration(key string, def time.Duration) (time.Duration, error) {
	val, ok := lookupEnvTrim(key)
	if !ok {
		return def, nil
	}
	res, err := time.ParseDuration(val)
	if err != nil {
		return 0, fmt.Errorf("invalid duration: %w", err)
	}
	return res, nil
}
