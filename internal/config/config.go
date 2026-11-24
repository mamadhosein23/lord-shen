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

// Config - مقادیر نهایی و Immutable برای اپلیکیشن
type Config struct {
	Addr            string
	RedisAddr       string
	Rate            float64
	Burst           int
	KeyTTL          time.Duration
	ShutdownTimeout time.Duration
}

type options struct {
	prefix string
}

type Option func(*options)

// WithPrefix اجازه می‌دهد متغیرها با پیشوند خاصی خوانده شوند (مثلاً "APP_")
func WithPrefix(prefix string) Option {
	return func(o *options) {
		o.prefix = prefix
	}
}

// Load پیکربندی را از Env می‌خواند و اعتبارسنجی می‌کند
func Load(opts ...Option) (*Config, error) {
	o := &options{prefix: ""}
	for _, opt := range opts {
		opt(o)
	}

	var errs []error
	read := func(key string) string { return o.prefix + key }

	rate, err := getEnvFloat(read("RATE"), 5.0)
	if err != nil {
		errs = append(errs, err)
	}

	burst, err := getEnvInt(read("BURST"), 20)
	if err != nil {
		errs = append(errs, err)
	}

	keyTTL, err := getEnvDuration(read("KEY_TTL"), 10*time.Minute)
	if err != nil {
		errs = append(errs, err)
	}

	shutdown, err := getEnvDuration(read("SHUTDOWN_TIMEOUT"), 5*time.Second)
	if err != nil {
		errs = append(errs, err)
	}

	cfg := &Config{
		Addr:            getEnv(read("ADDR"), ":8080"),
		RedisAddr:       getEnv(read("REDIS_ADDR"), "localhost:6379"),
		Rate:            rate,
		Burst:           burst,
		KeyTTL:          keyTTL,
		ShutdownTimeout: shutdown,
	}

	if verr := cfg.validate(); verr != nil {
		errs = append(errs, verr)
	}

	if len(errs) > 0 {
		return nil, fmt.Errorf("config loading failed:\n%w", errors.Join(errs...))
	}

	return cfg, nil
}

// validate اعتبارسنجی دامنه و مقادیر منطقی را انجام می‌دهد
func (c *Config) validate() error {
	var errs []error

	if err := validateNetworkAddr("ADDR", c.Addr); err != nil {
		errs = append(errs, err)
	}
	if err := validateNetworkAddr("REDIS_ADDR", c.RedisAddr); err != nil {
		errs = append(errs, err)
	}
	if math.IsNaN(c.Rate) || math.IsInf(c.Rate, 0) || c.Rate <= 0 {
		errs = append(errs, fmt.Errorf("RATE must be positive, got %v", c.Rate))
	}
	if c.Burst <= 0 {
		errs = append(errs, fmt.Errorf("BURST must be > 0, got %d", c.Burst))
	}
	if c.KeyTTL <= 0 {
		errs = append(errs, fmt.Errorf("KEY_TTL must be > 0, got %v", c.KeyTTL))
	}
	if c.ShutdownTimeout <= 0 {
		errs = append(errs, fmt.Errorf("SHUTDOWN_TIMEOUT must be > 0, got %v", c.ShutdownTimeout))
	}

	return errors.Join(errs...)
}

func validateNetworkAddr(key, addr string) error {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("%s: invalid format (must be host:port or :port): %w", key, err)
	}
	p, err := strconv.Atoi(port)
	if err != nil || p <= 0 || p > 65535 {
		return fmt.Errorf("%s: invalid port %q (must be 1-65535)", key, port)
	}
	_ = host // میزبان می‌تواند خالی باشد (مانند ":8080")
	return nil
}

// --- Env Helpers ---

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
	n, err := strconv.Atoi(strings.TrimSpace(val))
	if err != nil {
		return 0, fmt.Errorf("%s: invalid integer %q", key, val)
	}
	return n, nil
}

func getEnvFloat(key string, def float64) (float64, error) {
	val, ok := os.LookupEnv(key)
	if !ok || strings.TrimSpace(val) == "" {
		return def, nil
	}
	f, err := strconv.ParseFloat(strings.TrimSpace(val), 64)
	if err != nil {
		return 0, fmt.Errorf("%s: invalid float %q", key, val)
	}
	return f, nil
}

func getEnvDuration(key string, def time.Duration) (time.Duration, error) {
	val, ok := os.LookupEnv(key)
	if !ok || strings.TrimSpace(val) == "" {
		return def, nil
	}
	d, err := time.ParseDuration(strings.TrimSpace(val))
	if err != nil {
		return 0, fmt.Errorf("%s: invalid duration %q (e.g. 5s, 10m)", key, val)
	}
	return d, nil
}
