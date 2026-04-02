package retry

import (
	"context"
	"math"
	"sync"
	"time"

	"datapipe/internal/common/errors"
)

type RetryPolicy interface {
	NextDelay(attempt int) time.Duration
	ShouldRetry(attempt int, err error) bool
	MaxAttempts() int
	MaxDuration() time.Duration
}

type ExponentialBackoff struct {
	initialInterval time.Duration
	maxInterval    time.Duration
	maxAttempts    int
	maxDuration    time.Duration
	multiplier     float64
}

func NewExponentialBackoff(initialInterval, maxInterval time.Duration, maxAttempts int, maxDuration time.Duration) *ExponentialBackoff {
	if initialInterval <= 0 {
		initialInterval = 100 * time.Millisecond
	}
	if maxInterval <= 0 {
		maxInterval = 60 * time.Second
	}
	if maxAttempts <= 0 {
		maxAttempts = 3
	}

	return &ExponentialBackoff{
		initialInterval: initialInterval,
		maxInterval:    maxInterval,
		maxAttempts:   maxAttempts,
		maxDuration:   maxDuration,
		multiplier:     2.0,
	}
}

func (e *ExponentialBackoff) NextDelay(attempt int) time.Duration {
	if attempt <= 0 {
		attempt = 1
	}

	interval := float64(e.initialInterval) * math.Pow(e.multiplier, float64(attempt-1))

	if interval > float64(e.maxInterval) {
		interval = float64(e.maxInterval)
	}

	jitter := time.Duration(float64(time.Nanosecond) * (float64(time.Now().UnixNano()%1000) / 1000.0))
	interval = interval * (1 + float64(jitter)/float64(time.Second))

	return time.Duration(interval)
}

func (e *ExponentialBackoff) ShouldRetry(attempt int, err error) bool {
	if err == nil {
		return true
	}

	if e.maxAttempts > 0 && attempt >= e.maxAttempts {
		return false
	}

	if de, ok := err.(*errors.Error); ok {
		if de.Code.IsFatal() {
			return false
		}
	}

	return true
}

func (e *ExponentialBackoff) MaxAttempts() int {
	return e.maxAttempts
}

func (e *ExponentialBackoff) MaxDuration() time.Duration {
	return e.maxDuration
}

type FixedBackoff struct {
	interval    time.Duration
	maxAttempts int
	maxDuration time.Duration
}

func NewFixedBackoff(interval time.Duration, maxAttempts int, maxDuration time.Duration) *FixedBackoff {
	if interval <= 0 {
		interval = 1 * time.Second
	}
	if maxAttempts <= 0 {
		maxAttempts = 3
	}

	return &FixedBackoff{
		interval:    interval,
		maxAttempts: maxAttempts,
		maxDuration: maxDuration,
	}
}

func (f *FixedBackoff) NextDelay(attempt int) time.Duration {
	return f.interval
}

func (f *FixedBackoff) ShouldRetry(attempt int, err error) bool {
	if err == nil {
		return true
	}

	if f.maxAttempts > 0 && attempt >= f.maxAttempts {
		return false
	}

	if de, ok := err.(*errors.Error); ok {
		if de.Code.IsFatal() {
			return false
		}
	}

	return true
}

func (f *FixedBackoff) MaxAttempts() int {
	return f.maxAttempts
}

func (f *FixedBackoff) MaxDuration() time.Duration {
	return f.maxDuration
}

type LinearBackoff struct {
	initialInterval time.Duration
	increment       time.Duration
	maxInterval     time.Duration
	maxAttempts     int
	maxDuration     time.Duration
}

func NewLinearBackoff(initialInterval, increment, maxInterval time.Duration, maxAttempts int, maxDuration time.Duration) *LinearBackoff {
	if initialInterval <= 0 {
		initialInterval = 100 * time.Millisecond
	}
	if increment <= 0 {
		increment = 100 * time.Millisecond
	}
	if maxInterval <= 0 {
		maxInterval = 60 * time.Second
	}
	if maxAttempts <= 0 {
		maxAttempts = 3
	}

	return &LinearBackoff{
		initialInterval: initialInterval,
		increment:       increment,
		maxInterval:     maxInterval,
		maxAttempts:     maxAttempts,
		maxDuration:     maxDuration,
	}
}

func (l *LinearBackoff) NextDelay(attempt int) time.Duration {
	if attempt <= 0 {
		attempt = 1
	}

	interval := float64(l.initialInterval) + float64(l.increment)*float64(attempt-1)

	if interval > float64(l.maxInterval) {
		interval = float64(l.maxInterval)
	}

	return time.Duration(interval)
}

func (l *LinearBackoff) ShouldRetry(attempt int, err error) bool {
	if err == nil {
		return true
	}

	if l.maxAttempts > 0 && attempt >= l.maxAttempts {
		return false
	}

	if de, ok := err.(*errors.Error); ok {
		if de.Code.IsFatal() {
			return false
		}
	}

	return true
}

func (l *LinearBackoff) MaxAttempts() int {
	return l.maxAttempts
}

func (l *LinearBackoff) MaxDuration() time.Duration {
	return l.maxDuration
}

type RetryConfig struct {
	InitialInterval time.Duration `json:"initial_interval"`
	MaxInterval    time.Duration `json:"max_interval"`
	MaxAttempts    int           `json:"max_attempts"`
	MaxDuration    time.Duration `json:"max_duration"`
	Multiplier     float64       `json:"multiplier"`
	BackoffType    string        `json:"backoff_type"`
}

func NewRetryConfig() *RetryConfig {
	return &RetryConfig{
		InitialInterval: 100 * time.Millisecond,
		MaxInterval:    60 * time.Second,
		MaxAttempts:    3,
		MaxDuration:    5 * time.Minute,
		Multiplier:     2.0,
		BackoffType:    "exponential",
	}
}

func NewRetryPolicyFromConfig(config *RetryConfig) RetryPolicy {
	switch config.BackoffType {
	case "fixed":
		return NewFixedBackoff(config.InitialInterval, config.MaxAttempts, config.MaxDuration)
	case "linear":
		return NewLinearBackoff(config.InitialInterval, config.InitialInterval*time.Duration(config.Multiplier), config.MaxInterval, config.MaxAttempts, config.MaxDuration)
	case "exponential":
		return NewExponentialBackoff(config.InitialInterval, config.MaxInterval, config.MaxAttempts, config.MaxDuration)
	default:
		return NewExponentialBackoff(config.InitialInterval, config.MaxInterval, config.MaxAttempts, config.MaxDuration)
	}
}

type RetryResult struct {
	Success   bool
	Value     interface{}
	Error     error
	Attempts  int
	Durations []time.Duration
}

func (r *RetryResult) TotalDuration() time.Duration {
	var total time.Duration
	for _, d := range r.Durations {
		total += d
	}
	return total
}

type RetryFunc func(ctx context.Context) (interface{}, error)

func Do(ctx context.Context, policy RetryPolicy, fn RetryFunc) *RetryResult {
	result := &RetryResult{
		Durations: make([]time.Duration, 0),
	}

	var lastErr error
	attempt := 0
	startTime := time.Now()

	for {
		attempt++

		if policy.MaxAttempts() > 0 && attempt > policy.MaxAttempts() {
			break
		}

		if policy.MaxDuration() > 0 && time.Since(startTime) > policy.MaxDuration() {
			break
		}

		select {
		case <-ctx.Done():
			result.Error = ctx.Err()
			return result
		default:
		}

		delayStart := time.Now()
		delay := policy.NextDelay(attempt)

		if delay > 0 {
			select {
			case <-ctx.Done():
				result.Error = ctx.Err()
				return result
			case <-time.After(delay):
			}
		}

		result.Durations = append(result.Durations, time.Since(delayStart))

		value, err := fn(ctx)
		lastErr = err

		if err == nil {
			result.Success = true
			result.Value = value
			result.Attempts = attempt
			return result
		}

		if !policy.ShouldRetry(attempt, err) {
			result.Error = lastErr
			result.Attempts = attempt
			return result
		}
	}

	result.Error = lastErr
	result.Attempts = attempt
	return result
}

type RetryManager struct {
	mu           sync.RWMutex
	policies     map[string]RetryPolicy
	defaultPolicy RetryPolicy
}

func NewRetryManager(defaultPolicy RetryPolicy) *RetryManager {
	if defaultPolicy == nil {
		defaultPolicy = NewExponentialBackoff(100*time.Millisecond, 60*time.Second, 3, 5*time.Minute)
	}

	return &RetryManager{
		policies:     make(map[string]RetryPolicy),
		defaultPolicy: defaultPolicy,
	}
}

func (m *RetryManager) RegisterPolicy(name string, policy RetryPolicy) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.policies[name] = policy
}

func (m *RetryManager) GetPolicy(name string) RetryPolicy {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if policy, ok := m.policies[name]; ok {
		return policy
	}

	return m.defaultPolicy
}

func (m *RetryManager) SetDefaultPolicy(policy RetryPolicy) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.defaultPolicy = policy
}

func (m *RetryManager) Execute(ctx context.Context, policyName string, fn RetryFunc) *RetryResult {
	policy := m.GetPolicy(policyName)
	return Do(ctx, policy, fn)
}

func (m *RetryManager) ExecuteWithDefault(ctx context.Context, fn RetryFunc) *RetryResult {
	return Do(ctx, m.defaultPolicy, fn)
}

type Attempt struct {
	Number      int
	StartTime   time.Time
	EndTime     time.Time
	Error       error
	Value       interface{}
	IsRetriable bool
}

func (a *Attempt) Duration() time.Duration {
	if a.EndTime.IsZero() {
		return 0
	}
	return a.EndTime.Sub(a.StartTime)
}

type RetryState struct {
	mu            sync.Mutex
	attempts      []*Attempt
	currentPolicy RetryPolicy
	maxValue      interface{}
}

func NewRetryState(policy RetryPolicy) *RetryState {
	return &RetryState{
		attempts:      make([]*Attempt, 0),
		currentPolicy: policy,
	}
}

func (s *RetryState) RecordAttempt(attempt *Attempt) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.attempts = append(s.attempts, attempt)
}

func (s *RetryState) GetAttempts() []*Attempt {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]*Attempt, len(s.attempts))
	copy(result, s.attempts)
	return result
}

func (s *RetryState) AttemptCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.attempts)
}

func (s *RetryState) TotalDuration() time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()

	var total time.Duration
	for _, a := range s.attempts {
		total += a.Duration()
	}
	return total
}

func (s *RetryState) LastAttempt() *Attempt {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.attempts) == 0 {
		return nil
	}
	return s.attempts[len(s.attempts)-1]
}

func (s *RetryState) ShouldContinue() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	lastAttempt := s.LastAttempt()
	if lastAttempt == nil {
		return true
	}

	if s.currentPolicy.MaxAttempts() > 0 && len(s.attempts) >= s.currentPolicy.MaxAttempts() {
		return false
	}

	if lastAttempt.Error != nil {
		return s.currentPolicy.ShouldRetry(len(s.attempts), lastAttempt.Error)
	}

	return true
}

func (s *RetryState) GetValue() interface{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.maxValue
}

func (s *RetryState) SetValue(value interface{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.maxValue = value
}

type RetryHandler struct {
	policy    RetryPolicy
	onRetry   func(attempt int, err error)
	onFail    func(err error)
	onSuccess func(value interface{})
}

func NewRetryHandler(policy RetryPolicy) *RetryHandler {
	return &RetryHandler{
		policy: policy,
	}
}

func (h *RetryHandler) WithOnRetry(fn func(attempt int, err error)) *RetryHandler {
	h.onRetry = fn
	return h
}

func (h *RetryHandler) WithOnFail(fn func(err error)) *RetryHandler {
	h.onFail = fn
	return h
}

func (h *RetryHandler) WithOnSuccess(fn func(value interface{})) *RetryHandler {
	h.onSuccess = fn
	return h
}

func (h *RetryHandler) Handle(ctx context.Context, fn RetryFunc) *RetryResult {
	return Do(ctx, h.policy, func(innerCtx context.Context) (interface{}, error) {
		value, err := fn(innerCtx)

		if err != nil && h.onRetry != nil {
			h.onRetry(1, err)
		}

		return value, err
	})
}

type BackoffCalculator struct {
	initialInterval time.Duration
	maxInterval     time.Duration
	multiplier      float64
	jitter          bool
}

func NewBackoffCalculator(initialInterval, maxInterval time.Duration, multiplier float64) *BackoffCalculator {
	if multiplier <= 0 {
		multiplier = 2.0
	}

	return &BackoffCalculator{
		initialInterval: initialInterval,
		maxInterval:     maxInterval,
		multiplier:      multiplier,
		jitter:          true,
	}
}

func (c *BackoffCalculator) Calculate(attempt int) time.Duration {
	if attempt <= 0 {
		attempt = 1
	}

	interval := float64(c.initialInterval) * math.Pow(c.multiplier, float64(attempt-1))

	if interval > float64(c.maxInterval) {
		interval = float64(c.maxInterval)
	}

	if c.jitter {
		jitterFactor := 0.5 + (float64(time.Now().UnixNano()%1000) / 1000.0)
		interval = interval * jitterFactor
	}

	return time.Duration(interval)
}

func (c *BackoffCalculator) SetJitter(enabled bool) {
	c.jitter = enabled
}
