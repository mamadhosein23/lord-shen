package config

import (
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds the application configuration parameters for HTTP and Limiter engines.
type Config struct {
	Addr            string        // Server listener address (e.g., ":8080" or "0.0.0.0:8080")
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

	if c.Rate <= 0 {
		errs = append(errs, fmt.Errorf("rate must be > 0, got %f", c.Rate))
	}
	if c.Burst <= 0 {
		errs = append(errs, fmt.Errorf("burst must be > 0, got %d", c.Burst))
	}

	// Token bucket consistency check: TTL must be longer than full refill time
	if c.Rate > 0 && c.Burst > 0 {
		minRefillDuration := time.Duration((float64(c.Burst)/c.Rate)*float64(time.Second))
		if c.KeyTTL < minRefillDuration {
			errs = append(errs, fmt.Errorf("key_ttl (%v) must be >= bucket refill time (%v)", c.KeyTTL, minRefillDuration))
		}
	}

	if c.ShutdownTimeout <= 0 {
		errs = append(errs, errors.New("shutdown_timeout must be positive"))
	}

	// Fast validation for TCP address format
	if _, _, err := net.SplitHostPort(ensurePortColon(c.Addr)); err != nil {
		errs = append(errs, fmt.Errorf("invalid listener addr %q: %w", c.Addr, err))
	}

	return errors.Join(errs...)
}

func ensurePortColon(addr string) string {
	if !strings.Contains(addr, ":") {
		return ":" + addr
	}
	return addr
}

func getEnv(key, def string) string {
	if val, ok := os.LookupEnv(key); ok && strings.TrimSpace(val) != "" {
		return strings.TrimSpace(val)
	}
	return def
}

func getEnvInt(key string, def int) (int, error) {
	val, ok := os.LookupEnv(key)
	if !ok || strings.TrimSpace(val) == "" {
		return def, nil
	}
	res, err := strconv.Atoi(strings.TrimSpace(val))
	if err != nil {
		return 0, fmt.Errorf("invalid int: %w", err)
	}
	return res, nil
}

func getEnvFloat(key string, def float64) (float64, error) {
	val, ok := os.LookupEnv(key)
	if !ok || strings.TrimSpace(val) == "" {
		return def, nil
	}
	res, err := strconv.ParseFloat(strings.TrimSpace(val), 64)
	if err != nil {
		return 0, fmt.Errorf("invalid float: %w", err)
	}
	return res, nil
}

func getEnvDuration(key string, def time.Duration) (time.Duration, error) {
	val, ok := os.LookupEnv(key)
	if !ok || strings.TrimSpace(val) == "" {
		return def, nil
	}
	res, err := time.ParseDuration(strings.TrimSpace(val))
	if err != nil {
		return 0, fmt.Errorf("invalid duration: %w", err)
	}
	return res, nil
}
