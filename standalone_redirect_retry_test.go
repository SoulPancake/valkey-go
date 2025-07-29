package valkey

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestStandaloneRedirectRetryWithContextDeadline(t *testing.T) {
	defer ShouldNotLeak(SetupLeakDetection())

	redirectErr := ValkeyError(strmsg('-', "REDIRECT 127.0.0.1:6380"))
	primaryConn := &mockConn{
		DialFn: func() error { return nil },
		DoFn: func(cmd Completed) ValkeyResult {
			return newErrResult(&redirectErr)
		},
	}

	redirectConn := &mockConn{
		DialFn: func() error { return errors.New("connection failed") },
	}

	s, err := newStandaloneClient(&ClientOption{
		InitAddress: []string{"primary"},
		Standalone: StandaloneOption{
			EnableRedirect: true,
		},
		DisableRetry: false, // Enable retry to test retry handler
	}, func(dst string, opt *ClientOption) conn {
		if dst == "primary" {
			return primaryConn
		}
		return redirectConn
	}, newRetryer(defaultRetryDelayFn))

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer s.Close()

	// Create context with short deadline
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	result := s.Do(ctx, s.B().Get().Key("test").Build())
	elapsed := time.Since(start)

	// Should have tried multiple times before giving up due to context deadline
	if elapsed < 50*time.Millisecond {
		t.Errorf("expected retries to take some time, got %v", elapsed)
	}

	// Should eventually return the original redirect error
	if result.Error() == nil {
		t.Error("expected error after context deadline")
	}

	if verr, ok := result.Error().(*ValkeyError); !ok || !strings.Contains(verr.Error(), "REDIRECT") {
		t.Errorf("expected REDIRECT error, got %v", result.Error())
	}
}

func TestStandaloneRedirectEventualSuccess(t *testing.T) {
	defer ShouldNotLeak(SetupLeakDetection())

	redirectErr := ValkeyError(strmsg('-', "REDIRECT 127.0.0.1:6380"))
	primaryConn := &mockConn{
		DialFn: func() error { return nil },
		DoFn: func(cmd Completed) ValkeyResult {
			return newErrResult(&redirectErr)
		},
	}

	attempt := 0
	redirectConn := &mockConn{
		DialFn: func() error {
			attempt++
			if attempt < 3 {
				return errors.New("connection failed")
			}
			return nil // Succeed on 3rd attempt
		},
		DoFn: func(cmd Completed) ValkeyResult {
			return ValkeyResult{val: strmsg('+', "OK")}
		},
		CloseFn: func() {},
	}

	s, err := newStandaloneClient(&ClientOption{
		InitAddress: []string{"primary"},
		Standalone: StandaloneOption{
			EnableRedirect: true,
		},
		DisableRetry: false, // Enable retry to test retry handler
	}, func(dst string, opt *ClientOption) conn {
		if dst == "primary" {
			return primaryConn
		}
		return redirectConn
	}, newRetryer(defaultRetryDelayFn))

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer s.Close()

	ctx := context.Background()
	result := s.Do(ctx, s.B().Get().Key("test").Build())

	if result.Error() != nil {
		t.Errorf("expected success after retries, got: %v", result.Error())
	}

	if str, _ := result.ToString(); str != "OK" {
		t.Errorf("expected OK response after redirect success, got: %s", str)
	}

	// Verify that multiple attempts were made
	if attempt < 3 {
		t.Errorf("expected at least 3 connection attempts, got %d", attempt)
	}
}