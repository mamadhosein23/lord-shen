package config

import (
	"os"
	"strconv"
	"time"
)

type Config struct {
	Addr         string        // پورت سرور
	Rate         float64       // توکن بر ثانیه
	Burst        int           // ظرفیت باکت
	CleanupEvery time.Duration // فاصله پاکسازی
	KeyTTL       time.Duration // عمر کلید
}

func Load() Config {
	return Config{
		Addr:         env("ADDR", ":8080"),
		Rate:         envFloat("RATE", 5.0),  // پیش‌فرض: ۵ درخواست بر ثانیه
		Burst:        envInt("BURST", 10),    // پیش‌فرض: اجازه‌ی ۱۰ درخواست همزمان
		CleanupEvery: envDur("CLEANUP_EVERY", time.Minute),
		KeyTTL:       envDur("KEY_TTL", 10*time.Minute),
	}
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func envInt(k string, def int) int {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func envFloat(k string, def float64) float64 {
	if v := os.Getenv(k); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return def
}

func envDur(k string, def time.Duration) time.Duration {
	if v := os.Getenv(k); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}
