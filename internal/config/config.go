package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

type Config struct {
	Addr         string        // پورت یا آدرس لیسنر سرور
	Rate         float64       // نرخ تولید توکن در ثانیه
	Burst        int           // ظرفیت باکت
	CleanupEvery time.Duration // بازه زمانی پاکسازی کلیدهای منقضی‌شده
	KeyTTL       time.Duration // مدت زمان انقضای کلید
}

// Load تنظیمات را از متغیرهای محیطی خوانده و مقادیر را اعتبارسنجی می‌کند.
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

func getEnv(key, def string) string {
	if val, ok := os.LookupEnv(key); ok && val != "" {
		return val
	}
	return def
}

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
