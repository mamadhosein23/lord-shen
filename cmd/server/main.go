package main

import (
	"fmt"
	"net/http"
	"time"
)

func main() {
	mux := http.NewServeMux()

	// ایجاد لرد شن: حداکثر ۳ درخواست در هر ۱۰ ثانیه برای هر IP
	limiter := NewRateLimiter(3, 10*time.Second)

	// روت محافظت شده
	apiHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"success": true, "data": "خوش آمدید! از سد دفاعی لرد شن عبور کردید."}`))
	})

	mux.Handle("/api/data", limiter.Middleware(apiHandler))

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"message": "Lord Shen Rate Limiter Engine is running!"}`))
	})

	port := ":8080"
	fmt.Printf("Lord Shen is awakening on port %s...\n", port)
	if err := http.ListenAndServe(port, mux); err != nil {
		fmt.Printf("Server failed to start: %v\n", err)
	}
}
