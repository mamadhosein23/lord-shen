package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/mamadhosein23/lord-shen/internal/limiter"
	"github.com/mamadhosein23/lord-shen/internal/middleware"
	"github.com/redis/go-redis/v9"
)

type Config struct {
	HTTPAddr string

	RedisAddr string

	Capacity      int
	RatePerSecond float64

	ShutdownTimeout time.Duration
}

func main() {
	logger := log.New(os.Stdout, "", log.LstdFlags|log.LUTC|log.Lmicroseconds)

	cfg, err := loadConfig()
	if err != nil {
		logger.Fatalf("config error: %v", err)
	}

	// Root context canceled on SIGINT/SIGTERM
	rootCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Redis client
	rdb := redis.NewClient(&redis.Options{
		Addr: cfg.RedisAddr,

		PoolSize:     50,
		MinIdleConns: 10,

		DialTimeout:  2 * time.Second,
		ReadTimeout:  1 * time.Second,
		WriteTimeout: 1 * time.Second,

		// Production safety knobs (reasonable defaults)
		MaxRetries:      2,
		PoolTimeout:     2 * time.Second,
		ConnMaxIdleTime: 5 * time.Minute,
		ConnMaxLifetime: 30 * time.Minute,
	})
	defer func() { _ = rdb.Close() }()

	// Initial Redis check (non-fatal, but affects readiness)
	if err := pingRedis(rootCtx, rdb, 2*time.Second); err != nil {
		logger.Printf("[WARN] redis ping failed at startup: %v (service will start; /readyz will fail until redis is back)", err)
	}

	redisLimiter := limiter.NewRedisLimiter(rdb)
	limiterMw := middleware.RateLimit(redisLimiter, middleware.RateLimitConfig{
		Capacity:      cfg.Capacity,
		RatePerSecond: cfg.RatePerSecond,
	})

	mux := http.NewServeMux()

	// Liveness: process is alive
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"status": "ok"})
	})

	// Readiness: dependencies ready (redis)
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if err := pingRedis(r.Context(), rdb, 800*time.Millisecond); err != nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]any{
				"status": "not_ready",
				"redis":  err.Error(),
			})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"status": "ready"})
	})

	apiHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{
			"status":  "success",
			"message": "Request permitted by Lord Shen",
		})
	})

	mux.Handle("/api/v1/resource", limiterMw(withBasicAccessLog(logger, apiHandler)))

	srv := &http.Server{
		Addr:    cfg.HTTPAddr,
		Handler: mux,

		ReadHeaderTimeout: 3 * time.Second,
		ReadTimeout:       5 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       120 * time.Second,

		// (اختیاری) اگر TLS پشت reverse proxy داری:
		// ErrorLog: logger,
		BaseContext: func(net.Listener) context.Context { return rootCtx },
	}

	errCh := make(chan error, 1)
	go func() {
		logger.Printf("Lord-Shen starting on %s", srv.Addr)
		errCh <- srv.ListenAndServe()
	}()

	select {
	case <-rootCtx.Done():
		logger.Println("shutdown signal received")
	case err := <-errCh:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Fatalf("server failed: %v", err)
		}
	}

	// Graceful shutdown
	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()

	// Stop accepting new keep-alive connections ASAP
	srv.SetKeepAlivesEnabled(false)

	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Printf("[WARN] graceful shutdown failed: %v", err)
		_ = srv.Close()
	}

	logger.Println("Lord-Shen exited cleanly")
}

func loadConfig() (Config, error) {
	cfg := Config{
		HTTPAddr:        envString("HTTP_ADDR", ":8080"),
		RedisAddr:       envString("REDIS_ADDR", "localhost:6379"),
		Capacity:        envInt("RATE_LIMIT_CAPACITY", 20),
		RatePerSecond:   envFloat("RATE_LIMIT_RPS", 5.0),
		ShutdownTimeout: envDuration("SHUTDOWN_TIMEOUT", 5*time.Second),
	}

	if cfg.Capacity <= 0 {
		return Config{}, fmt.Errorf("RATE_LIMIT_CAPACITY must be > 0")
	}
	if cfg.RatePerSecond <= 0 {
		return Config{}, fmt.Errorf("RATE_LIMIT_RPS must be > 0")
	}
	return cfg, nil
}

func pingRedis(ctx context.Context, rdb *redis.Client, timeout time.Duration) error {
	pingCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return rdb.Ping(pingCtx).Err()
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func withBasicAccessLog(logger *log.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		logger.Printf("method=%s path=%s remote=%s ua=%q dur=%s",
			r.Method, r.URL.Path, r.RemoteAddr, r.UserAgent(), time.Since(start))
	})
}

func envString(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func envFloat(key string, def float64) float64 {
	if v := os.Getenv(key); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return def
}

func envDuration(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}
