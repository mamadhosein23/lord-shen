package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
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
	HTTPAddr        string
	RedisAddr       string
	Capacity        int
	RatePerSecond   float64
	ShutdownTimeout time.Duration
}

func main() {
	logger := log.New(os.Stdout, "", log.LstdFlags|log.LUTC|log.Lmicroseconds)

	cfg, err := loadConfig()
	if err != nil {
		logger.Fatalf("[FATAL] config error: %v", err)
	}

	rootCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	rdb := redis.NewClient(&redis.Options{
		Addr:            cfg.RedisAddr,
		PoolSize:        50,
		MinIdleConns:    10,
		DialTimeout:     2 * time.Second,
		ReadTimeout:     1 * time.Second,
		WriteTimeout:    1 * time.Second,
		MaxRetries:      2,
		PoolTimeout:     2 * time.Second,
		ConnMaxIdleTime: 5 * time.Minute,
		ConnMaxLifetime: 30 * time.Minute,
	})
	defer func() {
		if err := rdb.Close(); err != nil {
			logger.Printf("[WARN] error closing redis: %v", err)
		}
	}()

	if err := pingRedis(rootCtx, rdb, 2*time.Second); err != nil {
		logger.Printf("[WARN] redis ping failed at startup: %v (service will start; /readyz will fail until redis is back)", err)
	}

	redisLimiter := limiter.NewRedisLimiter(rdb)
	limiterMw := middleware.RateLimit(redisLimiter, middleware.RateLimitConfig{
		Capacity:      cfg.Capacity,
		RatePerSecond: cfg.RatePerSecond,
	})

	mux := http.NewServeMux()

	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"status": "ok"})
	})

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
		Addr:              cfg.HTTPAddr,
		Handler:           mux,
		ReadHeaderTimeout: 3 * time.Second,
		ReadTimeout:       5 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       120 * time.Second,
		// BaseContext حذف شد تا هنگام شات‌داون ریکوئست‌های فعال Cancel نشوند
	}

	errCh := make(chan error, 1)
	go func() {
		logger.Printf("Lord-Shen starting on %s", srv.Addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
		close(errCh)
	}()

	select {
	case <-rootCtx.Done():
		logger.Println("shutdown signal received")
	case err := <-errCh:
		if err != nil {
			logger.Fatalf("server failed to start: %v", err)
		}
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()

	srv.SetKeepAlivesEnabled(false)

	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Printf("[WARN] graceful shutdown failed: %v; forcing close", err)
		_ = srv.Close()
	}

	// صبر برای خروج تمیز گوروتین ListenAndServe
	<-errCh
	logger.Println("Lord-Shen exited cleanly")
}

func loadConfig() (Config, error) {
	capVal, err := envInt("RATE_LIMIT_CAPACITY", 20)
	if err != nil {
		return Config{}, err
	}
	if capVal <= 0 {
		return Config{}, fmt.Errorf("RATE_LIMIT_CAPACITY must be > 0")
	}

	rpsVal, err := envFloat("RATE_LIMIT_RPS", 5.0)
	if err != nil {
		return Config{}, err
	}
	if rpsVal <= 0 {
		return Config{}, fmt.Errorf("RATE_LIMIT_RPS must be > 0")
	}

	timeoutVal, err := envDuration("SHUTDOWN_TIMEOUT", 5*time.Second)
	if err != nil {
		return Config{}, err
	}

	return Config{
		HTTPAddr:        envString("HTTP_ADDR", ":8080"),
		RedisAddr:       envString("REDIS_ADDR", "localhost:6379"),
		Capacity:        capVal,
		RatePerSecond:   rpsVal,
		ShutdownTimeout: timeoutVal,
	}, nil
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

type statusResponseWriter struct {
	http.ResponseWriter
	statusCode int
}

func (lrw *statusResponseWriter) WriteHeader(code int) {
	lrw.statusCode = code
	lrw.ResponseWriter.WriteHeader(code)
}

func withBasicAccessLog(logger *log.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		wrapped := &statusResponseWriter{ResponseWriter: w, statusCode: http.StatusOK}
		next.ServeHTTP(wrapped, r)
		logger.Printf("status=%d method=%s path=%s remote=%s dur=%s",
			wrapped.statusCode, r.Method, r.URL.Path, r.RemoteAddr, time.Since(start))
	})
}

func envString(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) (int, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("invalid int for %s: %w", key, err)
	}
	return n, nil
}

func envFloat(key string, def float64) (float64, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid float for %s: %w", key, err)
	}
	return f, nil
}

func envDuration(key string, def time.Duration) (time.Duration, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("invalid duration for %s: %w", key, err)
	}
	return d, nil
}
