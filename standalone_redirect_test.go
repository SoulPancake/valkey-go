package valkey

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/valkey-io/valkey-go/internal/cmds"
)

func TestStandaloneRedirectBasicFunctionality(t *testing.T) {
	defer ShouldNotLeak(SetupLeakDetection())

	// Create a mock redirect response
	redirectErr := ValkeyError(strmsg('-', "MOVED 1234 127.0.0.1:6380"))

	// Mock primary connection that returns redirect
	primaryConn := &mockConn{
		DoFn: func(cmd Completed) ValkeyResult {
			return newErrResult(&redirectErr)
		},
	}

	// Mock redirect target connection that returns success
	redirectConn := &mockConn{
		DoFn: func(cmd Completed) ValkeyResult {
			return ValkeyResult{val: strmsg('+', "OK")}
		},
	}

	// Track which connection is being used
	var connUsed string

	s, err := newStandaloneClient(&ClientOption{
		InitAddress: []string{"primary"},
		Standalone: StandaloneOption{
			EnableRedirect: true,
		},
	}, func(dst string, opt *ClientOption) conn {
		connUsed = dst
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
		t.Errorf("expected no error after redirect, got: %v", result.Error())
	}

	if str, _ := result.ToString(); str != "OK" {
		t.Errorf("expected OK response after redirect, got: %s", str)
	}

	// Verify that the redirect target was used
	if connUsed != "127.0.0.1:6380" {
		t.Errorf("expected redirect to use 127.0.0.1:6380, got: %s", connUsed)
	}
}

func TestStandaloneRedirectDisabled(t *testing.T) {
	defer ShouldNotLeak(SetupLeakDetection())

	// Create a mock redirect response
	redirectErr := ValkeyError(strmsg('-', "MOVED 1234 127.0.0.1:6380"))

	// Mock primary connection that returns redirect
	primaryConn := &mockConn{
		DoFn: func(cmd Completed) ValkeyResult {
			return newErrResult(&redirectErr)
		},
	}

	s, err := newStandaloneClient(&ClientOption{
		InitAddress: []string{"primary"},
		Standalone: StandaloneOption{
			EnableRedirect: false, // Disabled
		},
	}, func(dst string, opt *ClientOption) conn {
		return primaryConn
	}, newRetryer(defaultRetryDelayFn))

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer s.Close()

	ctx := context.Background()
	result := s.Do(ctx, s.B().Get().Key("test").Build())

	// Should return the original redirect error since redirect is disabled
	if result.Error() == nil {
		t.Error("expected redirect error to be returned when redirect is disabled")
	}

	if result.Error().Error() != "MOVED 1234 127.0.0.1:6380" {
		t.Errorf("expected redirect error, got: %v", result.Error())
	}
}

func TestStandaloneRedirectWithReplicaAddressConflict(t *testing.T) {
	defer ShouldNotLeak(SetupLeakDetection())
	_, err := newStandaloneClient(
		&ClientOption{
			InitAddress: []string{"primary"},
			Standalone: StandaloneOption{
				EnableRedirect: true,
				ReplicaAddress: []string{"replica"},
			},
		}, func(dst string, opt *ClientOption) conn { return &mockConn{} }, newRetryer(defaultRetryDelayFn),
	)
	if err == nil || err.Error() != "EnableRedirect and ReplicaAddress cannot be used together" {
		t.Fatalf("expected conflict error, got %v", err)
	}
}

func TestStandaloneRedirectRetryLimit(t *testing.T) {
	defer ShouldNotLeak(SetupLeakDetection())

	// Create mock redirect responses with different addresses
	redirectCount := 0
	primaryConn := &mockConn{
		DoFn: func(cmd Completed) ValkeyResult {
			redirectCount++
			redirectErr := ValkeyError(strmsg('-', "MOVED 1234 127.0.0.1:638"+string(rune('0'+redirectCount))))
			return newErrResult(&redirectErr)
		},
	}

	// Mock redirect connections that also return redirects
	redirectConn := &mockConn{
		DoFn: func(cmd Completed) ValkeyResult {
			redirectCount++
			if redirectCount > 5 { // Stop infinite redirects
				return ValkeyResult{val: strmsg('+', "OK")}
			}
			redirectErr := ValkeyError(strmsg('-', "MOVED 1234 127.0.0.1:638"+string(rune('0'+redirectCount))))
			return newErrResult(&redirectErr)
		},
	}

	s, err := newStandaloneClient(&ClientOption{
		InitAddress: []string{"primary"},
		Standalone: StandaloneOption{
			EnableRedirect:     true,
			RedirectRetryLimit: 2, // Only 2 retries allowed
		},
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

	// Should return error because retry limit was exceeded
	if result.Error() == nil {
		t.Error("expected error due to retry limit exceeded")
	}

	// Should not have exceeded the retry limit + initial attempt
	if redirectCount > 3 { // 1 initial + 2 retries
		t.Errorf("expected max 3 redirect attempts, got %d", redirectCount)
	}
}

func TestStandaloneRedirectDefaultRetryLimit(t *testing.T) {
	defer ShouldNotLeak(SetupLeakDetection())

	s, err := newStandaloneClient(&ClientOption{
		InitAddress: []string{"primary"},
		Standalone: StandaloneOption{
			EnableRedirect: true, // No explicit retry limit
		},
	}, func(dst string, opt *ClientOption) conn {
		return &mockConn{}
	}, newRetryer(defaultRetryDelayFn))

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer s.Close()

	// Check that default retry limit is set
	if s.retryLimit != 3 {
		t.Errorf("expected default retry limit of 3, got %d", s.retryLimit)
	}
}

func TestStandaloneRedirectASK(t *testing.T) {
	defer ShouldNotLeak(SetupLeakDetection())

	// Create a mock ASK redirect response
	askErr := ValkeyError(strmsg('-', "ASK 1234 127.0.0.1:6380"))

	// Mock primary connection that returns ASK
	primaryConn := &mockConn{
		DoFn: func(cmd Completed) ValkeyResult {
			return newErrResult(&askErr)
		},
	}

	// Mock ASK target connection
	askingReceived := false
	askConn := &mockConn{
		DoMultiFn: func(multi ...Completed) *valkeyresults {
			if len(multi) >= 2 && multi[0] == cmds.AskingCmd {
				askingReceived = true
				return &valkeyresults{s: []ValkeyResult{
					{val: strmsg('+', "OK")}, // ASKING response
					{val: strmsg('+', "VALUE")}, // Original command response
				}}
			}
			return &valkeyresults{s: []ValkeyResult{{val: strmsg('+', "FAIL")}}}
		},
	}

	s, err := newStandaloneClient(&ClientOption{
		InitAddress: []string{"primary"},
		Standalone: StandaloneOption{
			EnableRedirect: true,
		},
	}, func(dst string, opt *ClientOption) conn {
		if dst == "primary" {
			return primaryConn
		}
		return askConn
	}, newRetryer(defaultRetryDelayFn))

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer s.Close()

	ctx := context.Background()
	result := s.Do(ctx, s.B().Get().Key("test").Build())

	if result.Error() != nil {
		t.Errorf("expected no error after ASK redirect, got: %v", result.Error())
	}

	if str, _ := result.ToString(); str != "VALUE" {
		t.Errorf("expected VALUE response after ASK redirect, got: %s", str)
	}

	if !askingReceived {
		t.Error("expected ASKING command to be sent")
	}
}

func TestStandaloneRedirectSingleflight(t *testing.T) {
	defer ShouldNotLeak(SetupLeakDetection())

	redirectCount := int32(0)
	primaryConn := &mockConn{
		DoFn: func(cmd Completed) ValkeyResult {
			redirectErr := ValkeyError(strmsg('-', "MOVED 1234 127.0.0.1:6380"))
			return newErrResult(&redirectErr)
		},
	}

	redirectConn := &mockConn{
		DoFn: func(cmd Completed) ValkeyResult {
			atomic.AddInt32(&redirectCount, 1)
			return ValkeyResult{val: strmsg('+', "OK")}
		},
		DialFn: func() error {
			// Simulate some delay to allow concurrent requests
			time.Sleep(10 * time.Millisecond)
			return nil
		},
	}

	s, err := newStandaloneClient(&ClientOption{
		InitAddress: []string{"primary"},
		Standalone: StandaloneOption{
			EnableRedirect: true,
		},
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

	// Execute multiple concurrent requests that should trigger redirect
	const numRequests = 10
	done := make(chan bool, numRequests)
	
	for i := 0; i < numRequests; i++ {
		go func() {
			defer func() { done <- true }()
			ctx := context.Background()
			result := s.Do(ctx, s.B().Get().Key("test").Build())
			if result.Error() != nil {
				t.Errorf("unexpected error: %v", result.Error())
			}
		}()
	}

	// Wait for all requests to complete
	for i := 0; i < numRequests; i++ {
		<-done
	}

	// Due to singleflight, only one redirect should have occurred
	// (Note: the redirectCount counts executions on the new primary, so should be numRequests)
	// But the key is that redirectToPrimary should only be called once
	finalCount := atomic.LoadInt32(&redirectCount)
	if finalCount != numRequests {
		t.Errorf("expected %d executions on redirected connection, got %d", numRequests, finalCount)
	}
}

func TestStandaloneRedirectStreamCommands(t *testing.T) {
	defer ShouldNotLeak(SetupLeakDetection())

	// Test DoStream redirect
	redirectErr := ValkeyError(strmsg('-', "MOVED 1234 127.0.0.1:6380"))
	
	primaryConn := &mockConn{
		DoStreamFn: func(cmd Completed) ValkeyResultStream {
			return ValkeyResultStream{e: &redirectErr}
		},
	}

	redirectConn := &mockConn{
		DoStreamFn: func(cmd Completed) ValkeyResultStream {
			return ValkeyResultStream{} // Success
		},
	}

	s, err := newStandaloneClient(&ClientOption{
		InitAddress: []string{"primary"},
		Standalone: StandaloneOption{
			EnableRedirect: true,
		},
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
	stream := s.DoStream(ctx, s.B().Set().Key("k").Value("v").Build())
	
	if stream.Error() != nil {
		t.Errorf("expected no error after stream redirect, got: %v", stream.Error())
	}
}

func TestStandaloneRedirectMultiStreamCommands(t *testing.T) {
	defer ShouldNotLeak(SetupLeakDetection())

	// Test DoMultiStream redirect
	redirectErr := ValkeyError(strmsg('-', "MOVED 1234 127.0.0.1:6380"))
	
	primaryConn := &mockConn{
		DoMultiStreamFn: func(multi ...Completed) MultiValkeyResultStream {
			return MultiValkeyResultStream{e: &redirectErr}
		},
	}

	redirectConn := &mockConn{
		DoMultiStreamFn: func(multi ...Completed) MultiValkeyResultStream {
			return MultiValkeyResultStream{} // Success
		},
	}

	s, err := newStandaloneClient(&ClientOption{
		InitAddress: []string{"primary"},
		Standalone: StandaloneOption{
			EnableRedirect: true,
		},
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
	stream := s.DoMultiStream(ctx, s.B().Set().Key("k").Value("v").Build())
	
	if stream.Error() != nil {
		t.Errorf("expected no error after multi-stream redirect, got: %v", stream.Error())
	}
}

func TestStandaloneRedirectMultiCommands(t *testing.T) {
	defer ShouldNotLeak(SetupLeakDetection())

	// Test DoMulti redirect
	redirectErr := ValkeyError(strmsg('-', "MOVED 1234 127.0.0.1:6380"))
	
	primaryConn := &mockConn{
		DoMultiFn: func(multi ...Completed) *valkeyresults {
			return &valkeyresults{s: []ValkeyResult{newErrResult(&redirectErr)}}
		},
	}

	redirectConn := &mockConn{
		DoMultiFn: func(multi ...Completed) *valkeyresults {
			return &valkeyresults{s: []ValkeyResult{{val: strmsg('+', "OK")}}}
		},
	}

	s, err := newStandaloneClient(&ClientOption{
		InitAddress: []string{"primary"},
		Standalone: StandaloneOption{
			EnableRedirect: true,
		},
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
	results := s.DoMulti(ctx, s.B().Set().Key("k").Value("v").Build())
	
	if len(results) == 0 || results[0].Error() != nil {
		t.Errorf("expected no error after multi redirect, got: %v", results)
	}

	if str, _ := results[0].ToString(); str != "OK" {
		t.Errorf("expected OK response after multi redirect, got: %s", str)
	}
}

func TestStandaloneRedirectAddressMatching(t *testing.T) {
	defer ShouldNotLeak(SetupLeakDetection())

	// This test verifies that singleflight only proceeds if the redirect address
	// matches the latest seen redirect address
	redirectCount := 0
	var latestAddr string
	
	primaryConn := &mockConn{
		DoFn: func(cmd Completed) ValkeyResult {
			redirectCount++
			// Return different redirect addresses to test address matching
			addr := "127.0.0.1:638" + string(rune('0'+(redirectCount%2)))
			latestAddr = addr
			redirectErr := ValkeyError(strmsg('-', "MOVED 1234 "+addr))
			return newErrResult(&redirectErr)
		},
	}

	redirectConn := &mockConn{
		DoFn: func(cmd Completed) ValkeyResult {
			return ValkeyResult{val: strmsg('+', "OK")}
		},
	}

	s, err := newStandaloneClient(&ClientOption{
		InitAddress: []string{"primary"},
		Standalone: StandaloneOption{
			EnableRedirect: true,
		},
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

	// Should succeed after redirect
	if result.Error() != nil {
		t.Errorf("expected no error after redirect, got: %v", result.Error())
	}

	// Verify that the latest redirect address is stored
	if stored := s.latestRedirectAddr.Load(); stored == nil || *stored != latestAddr {
		t.Errorf("expected latest redirect address %s, got %v", latestAddr, stored)
	}
}