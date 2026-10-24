package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"lord-shen/internal/config"
	"lord-shen/internal/limiter"
	"lord-shen/internal/middleware"
)

func main() {
	// Initialize structured JSON logger
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	// Load configuration from environment variables
	cfg, err := config.Load()
	if err != nil {
		logger.Error("failed to load configuration", slog.String("error", err.Error()))
		os.Exit(1)
	}

	// Initialize the TokenBucket engine
	engine := limiter.NewTokenBucket(limiter.Config{
		Rate:         cfg.Rate,
		Burst:        cfg.Burst,
		CleanupEvery: cfg.CleanupEvery,
		KeyTTL:       cfg.KeyTTL,
	})

	// Setup application router and apply rate limiting middleware
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"ok"}`))
	})
	mux.HandleFunc("GET /api/v1/resource", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"message":"access granted"}`))
	})

	handler := middleware.RateLimitMiddleware(engine, logger)(mux)

	// Configure HTTP server with production-grade timeouts
	server := &http.Server{
		Addr:         cfg.Addr,
		Handler:      handler,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Listen for OS signals for graceful shutdown
	shutdownCh := make(chan os.Signal, 1)
	signal.Notify(shutdownCh, os.Interrupt, syscall.SIGTERM)

	go func() {
		logger.Info("starting server", slog.String("addr", cfg.Addr), slog.Float64("rate", cfg.Rate), slog.Int("burst", cfg.Burst))
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("server listener encountered an error", slog.String("error", err.Error()))
			os.Exit(1)
		}
	}()

	// Wait for termination signal
	<-shutdownCh
	logger.Info("shutting down server gracefully...")

	// Create timeout context for server and limiter drain
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		logger.Error("server shutdown failed", slog.String("error", err.Error()))
	}

	// Terminate background cleanup janitor
	if err := engine.Close(); err != nil {
		logger.Error("limiter shutdown failed", slog.String("error", err.Error()))
	}

	logger.Info("server and limiter engine terminated cleanly")
}
