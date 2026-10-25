package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/mamadhosein23/lord-shen/internal/limiter"
	"github.com/mamadhosein23/lord-shen/internal/middleware"
	"github.com/redis/go-redis/v9"
)

func main() {
	// 1. Initialize Redis Client with production pooling
	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}

	rdb := redis.NewClient(&redis.Options{
		Addr:         redisAddr,
		PoolSize:     50,
		MinIdleConns: 10,
		DialTimeout:  2 * time.Second,
		ReadTimeout:  1 * time.Second,
		WriteTimeout: 1 * time.Second,
	})
	defer rdb.Close()

	// Verify Redis Ping
	pingCtx, cancelPing := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelPing()
	if err := rdb.Ping(pingCtx).Err(); err != nil {
		log.Printf("[WARN] Redis unreachable at start: %v. Running in degraded state.", err)
	}

	// 2. Setup Limiter Engine
	redisLimiter := limiter.NewRedisLimiter(rdb)

	limiterMw := middleware.RateLimit(redisLimiter, middleware.RateLimitConfig{
		Capacity:      20,  // Max burst
		RatePerSecond: 5.0, // Sustained rate
	})

	// 3. Setup Routes
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	})

	apiHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"success","message":"Request permitted by Lord Shen"}`))
	})

	mux.Handle("/api/v1/resource", limiterMw(apiHandler))

	// 4. HTTP Server Configuration
	srv := &http.Server{
		Addr:         ":8080",
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	// 5. Graceful Shutdown Listener
	stopCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		log.Printf("Lord-Shen service starting on %s ...", srv.Addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("Server failed: %v", err)
		}
	}()

	<-stopCtx.Done()
	log.Println("Shutdown signal received. Initiating graceful shutdown...")

	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelShutdown()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("Server forced shutdown: %v", err)
	}

	log.Println("Lord-Shen exited cleanly.")
}
