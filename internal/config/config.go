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

// Config - مقادیر نهایی و Immutable
type Config struct {
	Addr            string
	RedisAddr       string
	Rate            float64
	Burst           int
	KeyTTL          time.Duration
	ShutdownTimeout time.Duration
}

// options ساختار درونی برای مدیریت تنظیمات Load
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

// Load با الگوی Functional Options
func Load(opts ...Option) (*Config, error) {
	o := &options{prefix: ""} // پیش‌فرض بدون پیشوند
	for _, opt := range opts {
		opt(o)
	}

	var errs []error
	
	// تعریف helper محلی برای اعمال پیشوند
	read := func(key string) string { return o.prefix + key }

	// خواندن با prefix
	rate, err := getEnvFloat(read("RATE"), 5.0)
	if err != nil { errs = append(errs, err) }

	burst, err := getEnvInt(read("BURST"), 20)
	if err != nil { errs = append(errs, err) }

	keyTTL, err := getEnvDuration(read("KEY_TTL"), 10*time.Minute)
	if err != nil { errs = append(errs, err) }

	shutdown, err := getEnvDuration(read("SHUTDOWN_TIMEOUT"), 5*time.Second)
	if err != nil { errs = append(errs, err) }

	cfg := &Config{
		Addr:            getEnv(read("ADDR"), ":8080"),
		RedisAddr:       getEnv(read("REDIS_ADDR"), "localhost:6379"),
		Rate:            rate,
		Burst:           burst,
		KeyTTL:          keyTTL,
		ShutdownTimeout: shutdown,
	}

	// ترکیب اعتبارسنجی با خطاهای خواندن
	if verr := cfg.validate(); verr != nil {
		errs = append(errs, verr)
	}

	if len(errs) > 0 {
		return nil, flattenErrors(errs)
	}
	return cfg, nil
}

// validate - اعتبارسنجی جداگانه (Domain Validation)
func (c *Config) validate() error {
	var errs []error

	if math.IsNaN(c.Rate) || math.IsInf(c.Rate, 0) || c.Rate <= 0 {
		errs = append(errs, fmt.Errorf("RATE must be positive, got %v", c.Rate))
	}
	if c.Burst <= 0 {
		errs = append(errs, fmt.Errorf("BURST must be > 0, got %d", c.Burst))
	}
	if c.KeyTTL <= 0 {
		errs = append(errs, fmt.Errorf("KEY_TTL must be > 0, got %v", c.KeyTTL))
	}
    // ... ادامه اعتبارسنجی‌های قبلی با متد validateAddr
    
	return errors.Join(errs...)
}

// flattenErrors یک خطای تمیز و خوانا برمی‌گرداند
func flattenErrors(errs []error) error {
	var b strings.Builder
	b.WriteString("config loading failed: ")
	for i, err := range errs {
		if i > 0 { b.WriteString("; ") }
		b.WriteString(err.Error())
	}
	return errors.New(b.String())
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
	if !ok || strings.TrimSpace(val) == "" { return def, nil }
	n, err := strconv.Atoi(strings.TrimSpace(val))
	if err != nil { return 0, fmt.Errorf("%s: invalid integer %q", key, val) }
	return n, nil
}

// (متدهای Float و Duration هم مشابه Int با فرمت خطای اختصاصی)
