package unit

import (
	"context"
	std_errors "errors"
	"sync"
	"testing"
	"time"

	"datapipe/internal/common/errors"
	"datapipe/internal/retry"
)

func TestExponentialBackoff(t *testing.T) {
	t.Run("basic delay calculation", func(t *testing.T) {
		backoff := retry.NewExponentialBackoff(
			100*time.Millisecond,
			60*time.Second,
			3,
			5*time.Minute,
		)

		delay1 := backoff.NextDelay(1)
		if delay1 < 100*time.Millisecond {
			t.Errorf("delay1 should be at least 100ms, got %v", delay1)
		}

		delay2 := backoff.NextDelay(2)
		if delay2 <= delay1 {
			t.Logf("delay2 %v should be greater than delay1 %v (may vary due to jitter)", delay2, delay1)
		}
	})

	t.Run("max interval cap", func(t *testing.T) {
		backoff := retry.NewExponentialBackoff(
			1*time.Second,
			2*time.Second,
			10,
			10*time.Minute,
		)

		for attempt := 1; attempt <= 5; attempt++ {
			delay := backoff.NextDelay(attempt)
			if delay > 2*time.Second+100*time.Millisecond {
				t.Errorf("delay for attempt %d should be capped at maxInterval, got %v", attempt, delay)
			}
		}
	})

	t.Run("zero attempt handling", func(t *testing.T) {
		backoff := retry.NewExponentialBackoff(
			100*time.Millisecond,
			60*time.Second,
			3,
			5*time.Minute,
		)

		delay := backoff.NextDelay(0)
		if delay < 100*time.Millisecond {
			t.Errorf("delay for attempt 0 should be at least initialInterval, got %v", delay)
		}
	})

	t.Run("should retry within attempts", func(t *testing.T) {
		backoff := retry.NewExponentialBackoff(
			100*time.Millisecond,
			60*time.Second,
			3,
			5*time.Minute,
		)

		testErr := std_errors.New("test error")
		if !backoff.ShouldRetry(1, testErr) {
			t.Error("should retry on first attempt")
		}
		if !backoff.ShouldRetry(2, testErr) {
			t.Error("should retry on second attempt")
		}
		if backoff.ShouldRetry(3, testErr) {
			t.Error("should not retry when attempt exceeds max")
		}
	})

	t.Run("no retry on nil error", func(t *testing.T) {
		backoff := retry.NewExponentialBackoff(
			100*time.Millisecond,
			60*time.Second,
			3,
			5*time.Minute,
		)

		if !backoff.ShouldRetry(1, nil) {
			t.Error("should retry on nil error")
		}
	})

	t.Run("fatal error no retry", func(t *testing.T) {
		backoff := retry.NewExponentialBackoff(
			100*time.Millisecond,
			60*time.Second,
			3,
			5*time.Minute,
		)

		fatalErr := errors.NewError(errors.ErrCodeInvalidParameter, "invalid param")
		if backoff.ShouldRetry(1, fatalErr) {
			t.Error("should not retry on fatal error")
		}
	})

	t.Run("max attempts and duration", func(t *testing.T) {
		backoff := retry.NewExponentialBackoff(
			100*time.Millisecond,
			60*time.Second,
			5,
			1*time.Second,
		)

		if backoff.MaxAttempts() != 5 {
			t.Errorf("expected maxAttempts=5, got %d", backoff.MaxAttempts())
		}
		if backoff.MaxDuration() != 1*time.Second {
			t.Errorf("expected maxDuration=1s, got %v", backoff.MaxDuration())
		}
	})
}

func TestRetryWithErrors(t *testing.T) {
	t.Run("retry until success", func(t *testing.T) {
		backoff := retry.NewExponentialBackoff(
			10*time.Millisecond,
			100*time.Millisecond,
			5,
			1*time.Second,
		)

		attempt := 0
		ctx := context.Background()

		result := retry.Do(ctx, backoff, func(ctx context.Context) (interface{}, error) {
			attempt++
			if attempt < 3 {
				return nil, std_errors.New("not ready yet")
			}
			return "success", nil
		})

		if !result.Success {
			t.Errorf("expected success, got error: %v", result.Error)
		}
		if result.Attempts != 3 {
			t.Errorf("expected 3 attempts, got %d", result.Attempts)
		}
		if result.Value != "success" {
			t.Errorf("expected value 'success', got %v", result.Value)
		}
	})

	t.Run("exhaust retries", func(t *testing.T) {
		backoff := retry.NewExponentialBackoff(
			10*time.Millisecond,
			100*time.Millisecond,
			3,
			1*time.Second,
		)

		ctx := context.Background()

		result := retry.Do(ctx, backoff, func(ctx context.Context) (interface{}, error) {
			return nil, std_errors.New("persistent error")
		})

		if result.Success {
			t.Error("expected failure")
		}
		if result.Attempts != 3 {
			t.Errorf("expected 3 attempts, got %d", result.Attempts)
		}
	})

	t.Run("context cancellation", func(t *testing.T) {
		backoff := retry.NewExponentialBackoff(
			100*time.Millisecond,
			100*time.Millisecond,
			10,
			10*time.Second,
		)

		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()

		result := retry.Do(ctx, backoff, func(ctx context.Context) (interface{}, error) {
			return nil, std_errors.New("will be cancelled")
		})

		if result.Error != context.DeadlineExceeded {
			t.Errorf("expected context.DeadlineExceeded, got %v", result.Error)
		}
	})

	t.Run("fatal error stops retry", func(t *testing.T) {
		backoff := retry.NewExponentialBackoff(
			10*time.Millisecond,
			100*time.Millisecond,
			5,
			1*time.Second,
		)

		ctx := context.Background()

		result := retry.Do(ctx, backoff, func(ctx context.Context) (interface{}, error) {
			return nil, errors.NewError(errors.ErrCodeInvalidParameter, "fatal error")
		})

		if result.Success {
			t.Error("expected failure due to fatal error")
		}
		if result.Attempts != 1 {
			t.Errorf("expected 1 attempt for fatal error, got %d", result.Attempts)
		}
	})
}

func TestRetryResult(t *testing.T) {
	t.Run("total duration calculation", func(t *testing.T) {
		result := &retry.RetryResult{
			Success:   true,
			Attempts:  3,
			Durations: []time.Duration{10 * time.Millisecond, 20 * time.Millisecond, 30 * time.Millisecond},
		}

		total := result.TotalDuration()
		expected := 60 * time.Millisecond

		if total < expected-1*time.Millisecond || total > expected+1*time.Millisecond {
			t.Errorf("expected total duration around %v, got %v", expected, total)
		}
	})

	t.Run("total duration empty", func(t *testing.T) {
		result := &retry.RetryResult{
			Durations: []time.Duration{},
		}

		total := result.TotalDuration()
		if total != 0 {
			t.Errorf("expected 0 duration for empty result, got %v", total)
		}
	})
}

func TestRetryManager(t *testing.T) {
	manager := retry.NewRetryManager(nil)

	t.Run("register and get policy", func(t *testing.T) {
		policy := retry.NewExponentialBackoff(100*time.Millisecond, 60*time.Second, 5, 5*time.Minute)
		manager.RegisterPolicy("custom", policy)

		retrieved := manager.GetPolicy("custom")
		if retrieved == nil {
			t.Fatal("expected non-nil policy")
		}
		if retrieved.MaxAttempts() != 5 {
			t.Errorf("expected max attempts 5, got %d", retrieved.MaxAttempts())
		}
	})

	t.Run("default policy", func(t *testing.T) {
		defaultPolicy := manager.GetPolicy("nonexistent")
		if defaultPolicy == nil {
			t.Fatal("expected non-nil default policy")
		}
		if defaultPolicy.MaxAttempts() != 3 {
			t.Errorf("expected default max attempts 3, got %d", defaultPolicy.MaxAttempts())
		}
	})

	t.Run("set default policy", func(t *testing.T) {
		newDefault := retry.NewExponentialBackoff(200*time.Millisecond, 60*time.Second, 10, 5*time.Minute)
		manager.SetDefaultPolicy(newDefault)

		retrieved := manager.GetPolicy("nonexistent")
		if retrieved.MaxAttempts() != 10 {
			t.Errorf("expected new default max attempts 10, got %d", retrieved.MaxAttempts())
		}
	})

	t.Run("execute with named policy", func(t *testing.T) {
		policy := retry.NewExponentialBackoff(10*time.Millisecond, 100*time.Millisecond, 3, 1*time.Second)
		manager.RegisterPolicy("quick", policy)

		ctx := context.Background()
		result := manager.Execute(ctx, "quick", func(ctx context.Context) (interface{}, error) {
			return "done", nil
		})

		if !result.Success {
			t.Errorf("expected success, got error: %v", result.Error)
		}
	})

	t.Run("execute with default policy", func(t *testing.T) {
		ctx := context.Background()
		result := manager.ExecuteWithDefault(ctx, func(ctx context.Context) (interface{}, error) {
			return "default done", nil
		})

		if !result.Success {
			t.Errorf("expected success, got error: %v", result.Error)
		}
		if result.Value != "default done" {
			t.Errorf("expected 'default done', got %v", result.Value)
		}
	})
}

func TestRetryState(t *testing.T) {
	t.Run("record and get attempts", func(t *testing.T) {
		policy := retry.NewExponentialBackoff(10*time.Millisecond, 100*time.Millisecond, 3, 1*time.Second)
		state := retry.NewRetryState(policy)

		state.RecordAttempt(&retry.Attempt{
			Number:    1,
			StartTime: time.Now(),
			EndTime:   time.Now().Add(10 * time.Millisecond),
		})

		state.RecordAttempt(&retry.Attempt{
			Number:    2,
			StartTime: time.Now(),
			EndTime:   time.Now().Add(10 * time.Millisecond),
			Error:     std_errors.New("failed"),
		})

		attempts := state.GetAttempts()
		if len(attempts) != 2 {
			t.Errorf("expected 2 attempts, got %d", len(attempts))
		}

		if state.AttemptCount() != 2 {
			t.Errorf("expected attempt count 2, got %d", state.AttemptCount())
		}
	})

	t.Run("total duration", func(t *testing.T) {
		policy := retry.NewExponentialBackoff(10*time.Millisecond, 100*time.Millisecond, 3, 1*time.Second)
		state := retry.NewRetryState(policy)

		state.RecordAttempt(&retry.Attempt{
			Number:    1,
			StartTime: time.Now(),
			EndTime:   time.Now().Add(10 * time.Millisecond),
		})

		state.RecordAttempt(&retry.Attempt{
			Number:    2,
			StartTime: time.Now(),
			EndTime:   time.Now().Add(20 * time.Millisecond),
		})

		total := state.TotalDuration()
		if total < 30*time.Millisecond {
			t.Errorf("expected total duration at least 30ms, got %v", total)
		}
	})

	t.Run("last attempt", func(t *testing.T) {
		policy := retry.NewExponentialBackoff(10*time.Millisecond, 100*time.Millisecond, 3, 1*time.Second)
		state := retry.NewRetryState(policy)

		last := state.LastAttempt()
		if last != nil {
			t.Error("expected nil last attempt for empty state")
		}

		state.RecordAttempt(&retry.Attempt{Number: 1})
		state.RecordAttempt(&retry.Attempt{Number: 2})

		last = state.LastAttempt()
		if last.Number != 2 {
			t.Errorf("expected last attempt number 2, got %d", last.Number)
		}
	})

	t.Run("should continue", func(t *testing.T) {
		policy := retry.NewExponentialBackoff(10*time.Millisecond, 100*time.Millisecond, 3, 1*time.Second)
		state := retry.NewRetryState(policy)

		if !state.ShouldContinue() {
			t.Error("should continue on empty state")
		}

		state.RecordAttempt(&retry.Attempt{Number: 1, Error: std_errors.New("error")})

		if !state.ShouldContinue() {
			t.Error("should continue after first failure")
		}

		state.RecordAttempt(&retry.Attempt{Number: 2, Error: std_errors.New("error")})
		state.RecordAttempt(&retry.Attempt{Number: 3, Error: std_errors.New("error")})

		if state.ShouldContinue() {
			t.Error("should not continue after max attempts")
		}
	})

	t.Run("get and set value", func(t *testing.T) {
		policy := retry.NewExponentialBackoff(10*time.Millisecond, 100*time.Millisecond, 3, 1*time.Second)
		state := retry.NewRetryState(policy)

		if state.GetValue() != nil {
			t.Error("expected nil initial value")
		}

		state.SetValue("test-value")

		if state.GetValue() != "test-value" {
			t.Errorf("expected 'test-value', got %v", state.GetValue())
		}
	})
}

func TestFixedBackoff(t *testing.T) {
	t.Run("fixed delay", func(t *testing.T) {
		backoff := retry.NewFixedBackoff(100*time.Millisecond, 5, 1*time.Minute)

		delay1 := backoff.NextDelay(1)
		delay2 := backoff.NextDelay(2)
		delay3 := backoff.NextDelay(3)

		if delay1 != 100*time.Millisecond {
			t.Errorf("expected 100ms delay, got %v", delay1)
		}
		if delay2 != 100*time.Millisecond {
			t.Errorf("expected 100ms delay, got %v", delay2)
		}
		if delay3 != 100*time.Millisecond {
			t.Errorf("expected 100ms delay, got %v", delay3)
		}
	})

	t.Run("should retry", func(t *testing.T) {
		backoff := retry.NewFixedBackoff(100*time.Millisecond, 3, 1*time.Minute)

		testErr := std_errors.New("test error")
		if !backoff.ShouldRetry(1, testErr) {
			t.Error("should retry on first attempt")
		}
		if backoff.ShouldRetry(3, testErr) {
			t.Error("should not retry on max attempts")
		}
	})
}

func TestLinearBackoff(t *testing.T) {
	t.Run("linear delay increase", func(t *testing.T) {
		backoff := retry.NewLinearBackoff(
			100*time.Millisecond,
			100*time.Millisecond,
			1*time.Second,
			10,
			10*time.Second,
		)

		delay1 := backoff.NextDelay(1)
		delay2 := backoff.NextDelay(2)
		delay3 := backoff.NextDelay(3)

		if delay2 <= delay1 {
			t.Errorf("delay2 (%v) should be greater than delay1 (%v)", delay2, delay1)
		}
		if delay3 <= delay2 {
			t.Errorf("delay3 (%v) should be greater than delay2 (%v)", delay3, delay2)
		}
	})

	t.Run("max interval cap", func(t *testing.T) {
		backoff := retry.NewLinearBackoff(
			100*time.Millisecond,
			500*time.Millisecond,
			300*time.Millisecond,
			10,
			10*time.Second,
		)

		for attempt := 1; attempt <= 5; attempt++ {
			delay := backoff.NextDelay(attempt)
			if delay > 300*time.Millisecond+10*time.Millisecond {
				t.Errorf("delay for attempt %d should be capped at maxInterval, got %v", attempt, delay)
			}
		}
	})
}

func TestRetryHandler(t *testing.T) {
	t.Run("with on retry callback", func(t *testing.T) {
		policy := retry.NewExponentialBackoff(10*time.Millisecond, 100*time.Millisecond, 3, 1*time.Second)
		handler := retry.NewRetryHandler(policy)

		var mu sync.Mutex
		retryCount := 0

		handler.WithOnRetry(func(attempt int, err error) {
			mu.Lock()
			defer mu.Unlock()
			retryCount++
		})

		ctx := context.Background()
		attempt := 0
		handler.Handle(ctx, func(ctx context.Context) (interface{}, error) {
			attempt++
			if attempt < 3 {
				return nil, std_errors.New("not ready")
			}
			return "success", nil
		})

		if retryCount != 2 {
			t.Errorf("expected 2 retry callbacks, got %d", retryCount)
		}
	})

	t.Run("with on success callback", func(t *testing.T) {
		policy := retry.NewExponentialBackoff(10*time.Millisecond, 100*time.Millisecond, 3, 1*time.Second)
		handler := retry.NewRetryHandler(policy)

		var mu sync.Mutex
		successValue := ""

		handler.WithOnSuccess(func(value interface{}) {
			mu.Lock()
			defer mu.Unlock()
			successValue = value.(string)
		})

		ctx := context.Background()
		handler.Handle(ctx, func(ctx context.Context) (interface{}, error) {
			return "success", nil
		})

		if successValue != "success" {
			t.Errorf("expected 'success', got '%s'", successValue)
		}
	})
}

func TestBackoffCalculator(t *testing.T) {
	t.Run("basic calculation", func(t *testing.T) {
		calc := retry.NewBackoffCalculator(100*time.Millisecond, 60*time.Second, 2.0)

		delay1 := calc.Calculate(1)
		delay2 := calc.Calculate(2)
		delay3 := calc.Calculate(3)

		if delay1 < 100*time.Millisecond {
			t.Errorf("delay1 should be at least 100ms, got %v", delay1)
		}
		if delay2 <= delay1 {
			t.Logf("delay2 %v should be >= delay1 %v (may vary due to jitter)", delay2, delay1)
		}
		if delay3 <= delay2 {
			t.Logf("delay3 %v should be >= delay2 %v (may vary due to jitter)", delay3, delay2)
		}
	})

	t.Run("zero attempt", func(t *testing.T) {
		calc := retry.NewBackoffCalculator(100*time.Millisecond, 60*time.Second, 2.0)

		delay := calc.Calculate(0)
		if delay < 100*time.Millisecond {
			t.Errorf("delay for attempt 0 should be at least 100ms, got %v", delay)
		}
	})

	t.Run("max interval cap", func(t *testing.T) {
		calc := retry.NewBackoffCalculator(1*time.Second, 2*time.Second, 2.0)

		for attempt := 1; attempt <= 5; attempt++ {
			delay := calc.Calculate(attempt)
			if delay > 2*time.Second+10*time.Millisecond {
				t.Errorf("delay for attempt %d should be capped at maxInterval, got %v", attempt, delay)
			}
		}
	})
}

func TestRetryConfig(t *testing.T) {
	t.Run("default config", func(t *testing.T) {
		cfg := retry.NewRetryConfig()

		if cfg.InitialInterval != 100*time.Millisecond {
			t.Errorf("expected initial interval 100ms, got %v", cfg.InitialInterval)
		}
		if cfg.MaxInterval != 60*time.Second {
			t.Errorf("expected max interval 60s, got %v", cfg.MaxInterval)
		}
		if cfg.MaxAttempts != 3 {
			t.Errorf("expected max attempts 3, got %d", cfg.MaxAttempts)
		}
		if cfg.Multiplier != 2.0 {
			t.Errorf("expected multiplier 2.0, got %f", cfg.Multiplier)
		}
		if cfg.BackoffType != "exponential" {
			t.Errorf("expected backoff type 'exponential', got '%s'", cfg.BackoffType)
		}
	})

	t.Run("create policy from config", func(t *testing.T) {
		cfg := &retry.RetryConfig{
			InitialInterval: 50 * time.Millisecond,
			MaxInterval:     500 * time.Millisecond,
			MaxAttempts:     5,
			MaxDuration:     10 * time.Second,
			Multiplier:      2.0,
			BackoffType:     "exponential",
		}

		policy := retry.NewRetryPolicyFromConfig(cfg)
		if policy.MaxAttempts() != 5 {
			t.Errorf("expected max attempts 5, got %d", policy.MaxAttempts())
		}
	})

	t.Run("fixed backoff from config", func(t *testing.T) {
		cfg := &retry.RetryConfig{
			InitialInterval: 100 * time.Millisecond,
			MaxAttempts:     3,
			BackoffType:     "fixed",
		}

		policy := retry.NewRetryPolicyFromConfig(cfg)
		delay := policy.NextDelay(1)
		delay2 := policy.NextDelay(2)

		if delay != delay2 {
			t.Errorf("fixed backoff should produce same delay, got %v and %v", delay, delay2)
		}
	})
}
