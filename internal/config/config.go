// validate ensures all configuration parameters fall within safe production limits.
func (c *Config) validate() error {
	var errs []error

	// 1. Validate Rate (prevent <= 0, NaN, and Inf)
	if c.Rate <= 0 || math.IsNaN(c.Rate) || math.IsInf(c.Rate, 0) {
		errs = append(errs, fmt.Errorf("rate must be a positive finite number, got %f", c.Rate))
	}

	// 2. Validate Burst
	if c.Burst <= 0 {
		errs = append(errs, fmt.Errorf("burst must be > 0, got %d", c.Burst))
	}

	// 3. KeyTTL must be positive on its own, not only through the refill check
	if c.KeyTTL <= 0 {
		errs = append(errs, fmt.Errorf("key_ttl must be positive, got %v", c.KeyTTL))
	}

	// 4. Token bucket consistency check: TTL must be >= full refill time
	// Only meaningful when rate and burst are valid (checked above)
	if len(errs) == 0 {
		refillSec := float64(c.Burst) / c.Rate
		if refillSec > float64(math.MaxInt64/int64(time.Second)) {
			errs = append(errs, errors.New("burst/rate ratio creates duration overflow"))
		} else {
			minRefillDuration := time.Duration(refillSec * float64(time.Second))
			if c.KeyTTL < minRefillDuration {
				errs = append(errs, fmt.Errorf("key_ttl (%v) must be >= bucket refill time (%v)", c.KeyTTL, minRefillDuration))
			}
		}
	}

	// 5. Validate Timeouts
	if c.ShutdownTimeout <= 0 {
		errs = append(errs, errors.New("shutdown_timeout must be positive"))
	}

	// 6. Validate Network Addresses
	if err := validateAddr(c.Addr, true); err != nil {
		errs = append(errs, fmt.Errorf("invalid listener addr %q: %w", c.Addr, err))
	}

	if err := validateAddr(c.RedisAddr, false); err != nil {
		errs = append(errs, fmt.Errorf("invalid redis addr %q: %w", c.RedisAddr, err))
	}

	return errors.Join(errs...)
}

// validateAddr verifies host:port or :port specifications correctly.
func validateAddr(addr string, allowPortOnly bool) error {
	target := addr
	if allowPortOnly && !strings.Contains(target, ":") {
		target = ":" + target
	}
	_, port, err := net.SplitHostPort(target)
	if err != nil {
		return err
	}
	if port == "" {
		return errors.New("missing port in address")
	}
	p, err := strconv.Atoi(port)
	if err != nil || p < 1 || p > 65535 {
		return fmt.Errorf("invalid port %q: must be between 1 and 65535", port)
	}
	return nil
}
