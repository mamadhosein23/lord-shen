package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// Config holds the application configuration parameters.
type Config struct {
	Addr         string        // Server listener address or port
	Rate         float64       // Token generation rate per second (Token Bucket algorithm)
	Burst        int           // Maximum bucket capacity for burst traffic
	CleanupEvery time.Duration // Interval for purging expired rate-limiter keys from memory
	KeyTTL       time.Duration // Time-to-live duration before idle keys expire
}

// Load reads configuration values from environment variables with fallback defaults and validates them.
func Load() (*Config, error) {
	cfg := &Config{
		Addr: getEnv("ADDR", ":8080"),
	}

	var err error

	if cfg.Rate, err = getEnvFloat("RATE", 5.0); err != nil {
		return nil, fmt.Errorf("invalid RATE: %w", err)
	}

	if cfg.Burst, err = getEnvInt("BURST", 10); err != nil {
		return nil, fmt.Errorf("invalid BURST: %w", err)
	}

	if cfg.CleanupEvery, err = getEnvDuration("CLEANUP_EVERY", time.Minute); err != nil {
		return nil, fmt.Errorf("invalid CLEANUP_EVERY: %w", err)
	}

	if cfg.KeyTTL, err = getEnvDuration("KEY_TTL", 10*time.Minute); err != nil {
		return nil, fmt.Errorf("invalid KEY_TTL: %w", err)
	}

	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("config validation failed: %w", err)
	}

	return cfg, nil
}

// validate ensures that all configuration parameters fall within valid logical ranges.
func (c *Config) validate() error {
	if c.Rate <= 0 {
		return fmt.Errorf("rate must be greater than zero, got %f", c.Rate)
	}
	if c.Burst <= 0 {
		return fmt.Errorf("burst must be greater than zero, got %d", c.Burst)
	}
	if c.CleanupEvery <= 0 {
		return fmt.Errorf("cleanup interval must be positive")
	}
	if c.KeyTTL <= 0 {
		return fmt.Errorf("key TTL must be positive")
	}
	return nil
}

// getEnv retrieves a string environment variable or returns the default fallback value.
func getEnv(key, def string) string {
	if val, ok := os.LookupEnv(key); ok && val != "" {
		return val
	}
	return def
}

// getEnvInt retrieves an integer environment variable or returns the default fallback value.
func getEnvInt(key string, def int) (int, error) {
	val, ok := os.LookupEnv(key)
	if !ok || val == "" {
		return def, nil
	}
	res, err := strconv.Atoi(val)
	if err != nil {
		return 0, err
	}
	return res, nil
}

// getEnvFloat retrieves a floating-point environment variable or returns the default fallback value.
func getEnvFloat(key string, def float64) (float64, error) {
	val, ok := os.LookupEnv(key)
	if !ok || val == "" {
		return def, nil
	}
	res, err := strconv.ParseFloat(val, 64)
	if err != nil {
		return 0, err
	}
	return res, nil
}

// getEnvDuration retrieves a time.Duration environment variable or returns the default fallback value.
func getEnvDuration(key string, def time.Duration) (time.Duration, error) {
	val, ok := os.LookupEnv(key)
	if !ok || val == "" {
		return def, nil
	}
	res, err := time.ParseDuration(val)
	if err != nil {
		return 0, err
	}
	return res, nil
}
