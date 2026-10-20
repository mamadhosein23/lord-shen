package main

import (
	"log"
	"net/http"
	"os"

	"github.com/mamadhosein23/lord-shen/internal/config"
	"github.com/mamadhosein23/lord-shen/internal/limiter"
	"github.com/mamadhosein23/lord-shen/internal/middleware"
)

func main() {
	cfg := config.Load()

	lb := limiter.NewTokenBucket(limiter.Config{
		Rate:         cfg.Rate,
		Burst:        cfg.Burst,
		CleanupEvery: cfg.CleanupEvery,
		KeyTTL:       cfg.KeyTTL,
	})

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"status":"ok","message":"welcome behind Lord Shen 🦚"}`))
	})

	handler := middleware.RateLimit(lb, mux)

	port := os.Getenv("ADDR")
	if port == "" {
		port = cfg.Addr
	}

	log.Printf("🦚 Lord Shen listening on %s (rate=%.1f/s, burst=%d)", port, cfg.Rate, cfg.Burst)
	if err := http.ListenAndServe(port, handler); err != nil {
		log.Fatalf("server error: %v", err)
	}
}
