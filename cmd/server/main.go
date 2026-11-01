package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync/atomic"
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
	logger := log.New(
		os.Stdout,
		"",
		log.LstdFlags|log.LUTC|log.Lmicroseconds,
	)

	if err := run(logger); err != nil {
		logger.Printf("[ERROR] Lord-Shen stopped: %v", err)
		os.Exit(1)
	}
}

func run(logger *log.Logger) error {
	cfg, err := loadConfig()
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}

	signalCtx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
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
			logger.Printf("[WARN] closing Redis: %v", err)
		}
	}()

	if err := pingRedis(signalCtx, rdb, 2*time.Second); err != nil {
		logger.Printf("[WARN] Redis initial ping failed: %v", err)
	}

	if signalCtx.Err() != nil {
		logger.Println("shutdown signal received during startup")
		return nil
	}

	redisLimiter := limiter.NewRedisLimiter(rdb)
	limiterMw := middleware.RateLimit(
		redisLimiter,
		middleware.RateLimitConfig{
			Capacity:      cfg.Capacity,
			RatePerSecond: cfg.RatePerSecond,
		},
	)

	var draining atomic.Bool
	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})

	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		if draining.Load() {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "not_ready"})
			return
		}

		// کانتکست مستقل برای جلوگیری از تاثیر لغو درخواست کلاینت روی نتیجه بررسی ردیس
		if err := pingRedis(context.Background(), rdb, 800*time.Millisecond); err != nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{
				"status": "not_ready",
				"redis":  "unavailable",
			})
			return
		}

		writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
	})

	apiHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{
			"status":  "success",
			"message": "Request permitted by Lord Shen",
		})
	})

	mux.Handle(
		"/api/v1/resource",
		withBasicAccessLog(logger, limiterMw(apiHandler)),
	)

	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           mux,
		ReadHeaderTimeout: 3 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	listener, err := net.Listen("tcp", cfg.HTTPAddr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", cfg.HTTPAddr, err)
	}

	serveErrCh := make(chan error, 1)
	go func() {
		logger.Printf("Lord-Shen starting on %s", listener.Addr())
		serveErrCh <- srv.Serve(listener)
	}()

	var serveErr error
	var serveResultReceived bool

	select {
	case <-signalCtx.Done():
		logger.Println("shutdown signal received")
	case err := <-serveErrCh:
		serveResultReceived = true
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			serveErr = fmt.Errorf("HTTP server: %w", err)
			logger.Printf("[ERROR] %v", serveErr)
		}
	}

	draining.Store(true)
	srv.SetKeepAlivesEnabled(false)

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()

	shutdownErr := srv.Shutdown(shutdownCtx)
	if shutdownErr != nil {
		logger.Printf("[WARN] graceful shutdown failed: %v; forcing close", shutdownErr)
		if err := srv.Close(); err != nil {
			logger.Printf("[WARN] forcing HTTP server close: %v", err)
		}
	}

	if !serveResultReceived {
		if err := <-serveErrCh; err != nil && !errors.Is(err, http.ErrServerClosed) && serveErr == nil {
			serveErr = fmt.Errorf("HTTP server: %w", err)
		}
	}

	if shutdownErr != nil {
		return errors.Join(serveErr, fmt.Errorf("HTTP shutdown: %w", shutdownErr))
	}
	if serveErr != nil {
		return serveErr
	}

	logger.Println("Lord-Shen exited cleanly")
	return nil
}

func loadConfig() (Config, error) {
	capacity, err := envInt("RATE_LIMIT_CAPACITY", 20)
	if err != nil {
		return Config{}, err
	}
	if capacity <= 0 {
		return Config{}, fmt.Errorf("RATE_LIMIT_CAPACITY must be > 0")
	}

	rps, err := envFloat("RATE_LIMIT_RPS", 5.0)
	if err != nil {
		return Config{}, err
	}
	if rps <= 0 || math.IsNaN(rps) || math.IsInf(rps, 0) {
		return Config{}, fmt.Errorf("RATE_LIMIT_RPS must be finite and > 0")
	}

	shutdownTimeout, err := envDuration("SHUTDOWN_TIMEOUT", 5*time.Second)
	if err != nil {
		return Config{}, err
	}
	if shutdownTimeout <= 0 {
		return Config{}, fmt.Errorf("SHUTDOWN_TIMEOUT must be > 0")
	}

	return Config{
		HTTPAddr:        envString("HTTP_ADDR", ":8080"),
		RedisAddr:       envString("REDIS_ADDR", "localhost:6379"),
		Capacity:        capacity,
		RatePerSecond:   rps,
		ShutdownTimeout: shutdownTimeout,
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
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("[WARN] writing JSON response: %v", err)
	}
}

type statusResponseWriter struct {
	http.ResponseWriter
	statusCode  int
	wroteHeader bool
}

func (w *statusResponseWriter) WriteHeader(code int) {
	if code >= 100 && code < 200 && code != http.StatusSwitchingProtocols {
		w.ResponseWriter.WriteHeader(code)
		return
	}

	if w.wroteHeader {
		return
	}

	w.statusCode = code
	w.wroteHeader = true
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusResponseWriter) Write(p []byte) (int, error) {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	return w.ResponseWriter.Write(p)
}

// Unwrap اتصال مستقیم ResponseController به ResponseWriter زیرین را فراهم می‌کند.
// پیاده‌سازی متدهای Flush و Hijack به صورت دستی حذف شد تا Unwrap بدون شکست عمل کند.
func (w *statusResponseWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}

func withBasicAccessLog(logger *log.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		wrapped := &statusResponseWriter{
			ResponseWriter: w,
			statusCode:     http.StatusOK,
		}

		next.ServeHTTP(wrapped, r)

		logger.Printf(
			"status=%d method=%s path=%s remote=%s ua=%q dur=%s",
			wrapped.statusCode,
			r.Method,
			r.URL.Path,
			r.RemoteAddr,
			r.UserAgent(),
			time.Since(start),
		)
	})
}

func envString(key, def string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return def
}

func envInt(key string, def int) (int, error) {
	value := os.Getenv(key)
	if value == "" {
		return def, nil
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("%s must be an integer: %w", key, err)
	}
	return parsed, nil
}

func envFloat(key string, def float64) (float64, error) {
	value := os.Getenv(key)
	if value == "" {
		return def, nil
	}
	parsed, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return 0, fmt.Errorf("%s must be a float: %w", key, err)
	}
	return parsed, nil
}

func envDuration(key string, def time.Duration) (time.Duration, error) {
	value := os.Getenv(key)
	if value == "" {
		return def, nil
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("%s must be a duration: %w", key, err)
	}
	return parsed, nil
}
