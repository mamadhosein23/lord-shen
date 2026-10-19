# Lord Shen  🦚

> A high-performance, concurrent-safe rate limiting and traffic shaping engine built from scratch in Go.

---

## 🚀 About the Project
**Lord Shen** is designed to protect backend servers from overwhelming traffic, DDoS attacks, and sudden traffic spikes (like flash sales). Inspired by the rigorous control of Lord Shen from Kung Fu Panda 2, this engine regulates incoming requests per client IP to ensure system stability and high availability.

---

## 🛠️ Key Features
- **Zero Heavy Dependencies:** Built purely using Go's standard library (`net/http`).
- **Thread-Safe Architecture:** Utilizes `sync.Mutex` to safely manage concurrent requests and prevent Race Conditions in memory.
- **Fixed Window Algorithm:** Implements robust window-based request counting per client IP.
- **Standard Rate Limit Headers:** Returns standard HTTP headers (`X-RateLimit-Limit`, `X-RateLimit-Remaining`) for frontend integration.
- **JSON Error Responses:** Clean and structured JSON error payloads when limits are exceeded.

---

## ⚙️ Getting Started

### Prerequisites
Make sure you have [Go](https://golang.org/) installed on your machine.

### Installation & Run
1. Clone the repository:
```bash
   git clone https://github.com/mamadhosein23/lord-shen.git
   cd lord-shen
   
