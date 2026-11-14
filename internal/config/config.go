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

type Config struct {
	Addr            string
	RedisAddr       string
	Rate            float64
	Burst           int
	KeyTTL          time.Duration
	ShutdownTimeout time.Duration
}

func Load() (*Config, error) {
	var errs []error

	// Parse layer: هر مقدار یا معتبر است یا خطا جمع می‌شود.
	// در صورت خطا مقدار پیش‌فرض برگردانده می‌شود تا مقدار صفر بی‌سروصدا وارد نشود.
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

	// Addr و RedisAddr همیشه ساخته و معتبرسازی می‌شوند (نه فقط وقتی چیزی خطا نداده)
	cfg := &Config{
		Addr:            getEnv("ADDR", ":8080"),
		RedisAddr:       getEnv("REDIS_ADDR", "localhost:6379"),
		Rate:            rate,
		Burst:           burst,
		KeyTTL:          keyTTL,
		ShutdownTimeout: shutdownTimeout,
	}

	if verr := cfg.validate(); verr != nil {
		var joined interface{ Unwrap() []error }
		if errors.As(verr, &joined) {
			errs = append(errs, joined.Unwrap()...)
		} else {
			errs = append(errs, verr)
		}
	}

	if len(errs) > 0 {
		return nil, joinErrs(errs)
	}
	return cfg, nil
}

// joinErrs پیام‌ها را در یک خط جمع می‌کند تا لاگ‌های تک‌خطی سالم بمانند.
func joinErrs(errs []error) error {
	msgs := make([]string, 0, len(errs))
	for _, e := range errs {
		if e != nil {
			msgs = append(msgs, e.Error())
		}
	}
	return fmt.Errorf("config: %s", strings.Join(msgs, "; "))
}

func (c *Config) validate() error {
	var errs []error

	if err := validateRate(c.Rate); err != nil {
		errs = append(errs, err)
	}
	if c.Burst <= 0 {
		errs = append(errs, fmt.Errorf("burst must be > 0, got %d", c.Burst))
	}
	if c.KeyTTL <= 0 {
		errs = append(errs, fmt.Errorf("key_ttl must be positive, got %v", c.KeyTTL))
	}
	if c.ShutdownTimeout <= 0 {
		errs = append(errs, errors.New("shutdown_timeout must be positive"))
	}

	// فقط وقتی rate و burst معتبرند، ریاضیِ refill معنا دارد
	if len(errs) == 0 {
		if err := validateRefill(c.Rate, c.Burst, c.KeyTTL); err != nil {
			errs = append(errs, err)
		}
	}

	if err := validateAddr(c.Addr, true); err != nil {
		errs = append(errs, fmt.Errorf("invalid listener addr %q: %w", c.Addr, err))
	}
	if err := validateAddr(c.RedisAddr, false); err != nil {
		errs = append(errs, fmt.Errorf("invalid redis addr %q: %w", c.RedisAddr, err))
	}

	return errors.Join(errs...)
}

func validateRate(rate float64) error {
	if math.IsNaN(rate) || math.IsInf(rate, 0) || rate <= 0 {
		return fmt.Errorf("rate must be a positive finite number, got %v", rate)
	}
	return nil
}

func validateRefill(rate float64, burst int, ttl time.Duration) error {
	refillSec := float64(burst) / rate     // ممکن است Inf شود (rate بسیار کوچک)
	if math.IsInf(refillSec, 0) {
		return errors.New("rate too small: burst/rate ratio overflows")
	}
	const maxSec = float64(math.MaxInt64) / float64(time.Second) // ≈ 9.22e9
	if refillSec > maxSec {
		return fmt.Errorf("burst/rate ratio (%g s) creates duration overflow", refillSec)
	}
	minRefill := time.Duration(refillSec * float64(time.Second))
	if ttl < minRefill {
		return fmt.Errorf("key_ttl (%v) must be >= bucket refill time (%v)", ttl, minRefill)
	}
	return nil
}

func validateAddr(addr string, allowPortOnly bool) error {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return errors.New("address must not be empty")
	}

	// در حالت allowPortOnly، اگر ":" ندارد اما عدد است، ":عدد" بساز.
	// "8080" بدون ":" فقط برای listener مجاز است، اما Go با چنین آدرسی Listen نمی‌کند؛
	// پس تبدیل صریح انجام می‌دهیم تا رفتار runtime با validate یکی باشد.
	target := addr
	if allowPortOnly && !strings.Contains(target, ":") {
		target = ":" + target
	}

	host, port, err := net.SplitHostPort(target)
	if err != nil {
		return err
	}
	if port == "" {
		return errors.New("missing port in address")
	}
	if p, err := strconv.Atoi(port); err != nil || p < 1 || p > 65535 {
		return fmt.Errorf("invalid port %q: must be between 1 and 65535", port)
	}
	// no address passes SplitHostPort with a whitespace in host
	if strings.ContainsAny(host, " \t\n") {
		return fmt.Errorf("invalid host %q", host)
	}
	// IPv6 literal بدون براکت
	if strings.Count(host, ":") > 1 && !strings.HasPrefix(target, "[") {
		return fmt.Errorf("IPv6 host must be bracketed, got %q", host)
	}
	return nil
}

// ---------- env helpers ----------

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
	n, err := strconv.Atoi(val)
	if err != nil {
		return def, fmt.Errorf("invalid int %q: %w", val, err)
	}
	return n, nil
}

func getEnvFloat(key string, def float64) (float64, error) {
	val, ok := lookupEnvTrim(key)
	if !ok {
		return def, nil
	}
	f, err := strconv.ParseFloat(val, 64)
	if err != nil {
		return def, fmt.Errorf("invalid float %q: %w", val, err)
	}
	return f, nil
}

func getEnvDuration(key string, def time.Duration) (time.Duration, error) {
	val, ok := lookupEnvTrim(key)
	if !ok {
		return def, nil
	}
	d, err := time.ParseDuration(val)
	if err != nil {
		return def, fmt.Errorf("invalid duration %q: %w", val, err)
	}
	return d, nil
}
