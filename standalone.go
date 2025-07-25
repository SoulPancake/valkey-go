package valkey

import (
	"context"
	"errors"
	"math/rand/v2"
	"sync/atomic"
	"time"

	"github.com/valkey-io/valkey-go/internal/cmds"
)

func newStandaloneClient(opt *ClientOption, connFn connFn, retryer retryHandler) (*standalone, error) {
	if len(opt.InitAddress) == 0 {
		return nil, ErrNoAddr
	}
	
	// Validate that EnableRedirect and ReplicaAddress are not used together
	if opt.Standalone.EnableRedirect && len(opt.Standalone.ReplicaAddress) > 0 {
		return nil, errors.New("EnableRedirect and ReplicaAddress cannot be used together")
	}
	
	// Set default redirect retry limit if EnableRedirect is true and limit is not specified
	retryLimit := opt.Standalone.RedirectRetryLimit
	if opt.Standalone.EnableRedirect && retryLimit == 0 {
		retryLimit = 3 // Default retry limit
	}
	
	p := connFn(opt.InitAddress[0], opt)
	if err := p.Dial(); err != nil {
		return nil, err
	}
	s := &standalone{
		toReplicas:      opt.SendToReplicas,
		primary:         newSingleClientWithConn(p, cmds.NewBuilder(cmds.NoSlot), !opt.DisableRetry, opt.DisableCache, retryer, false),
		replicas:        make([]*singleClient, len(opt.Standalone.ReplicaAddress)),
		enableRedirect:  opt.Standalone.EnableRedirect,
		retryLimit:      retryLimit,
		connFn:          connFn,
		opt:             opt,
		retryer:         retryer,
	}
	opt.ReplicaOnly = true
	for i := range s.replicas {
		replicaConn := connFn(opt.Standalone.ReplicaAddress[i], opt)
		if err := replicaConn.Dial(); err != nil {
			s.primary.Close() // close primary if any replica fails
			for j := 0; j < i; j++ {
				s.replicas[j].Close()
			}
			return nil, err
		}
		s.replicas[i] = newSingleClientWithConn(replicaConn, cmds.NewBuilder(cmds.NoSlot), !opt.DisableRetry, opt.DisableCache, retryer, false)
	}
	return s, nil
}

type standalone struct {
	toReplicas      func(Completed) bool
	primary         *singleClient
	replicas        []*singleClient
	enableRedirect  bool
	retryLimit      int
	connFn          connFn
	opt             *ClientOption
	retryer         retryHandler
	redirectCall    call
	// latestRedirectAddr stores the latest redirect address atomically to ensure
	// singleflight operations only proceed if they match the current redirect target
	latestRedirectAddr atomic.Pointer[string]
}

func (s *standalone) B() Builder {
	return s.primary.B()
}

func (s *standalone) pick() int {
	if len(s.replicas) == 1 {
		return 0
	}
	return rand.IntN(len(s.replicas))
}

func (s *standalone) Do(ctx context.Context, cmd Completed) (resp ValkeyResult) {
	if s.toReplicas != nil && s.toReplicas(cmd) {
		return s.replicas[s.pick()].Do(ctx, cmd)
	}
	resp = s.primary.Do(ctx, cmd)
	
	// Handle redirects if enabled
	if s.enableRedirect {
		resp = s.handleRedirect(ctx, cmd, resp)
	}
	
	return resp
}

// handleRedirect processes MOVED and ASK redirects with singleflight coordination and retry logic
func (s *standalone) handleRedirect(ctx context.Context, cmd Completed, result ValkeyResult) ValkeyResult {
	if result.Error() == nil {
		return result
	}

	if ret, yes := IsValkeyErr(result.Error()); yes {
		var addr string
		var ok bool
		
		// Check for MOVED or ASK redirects
		if addr, ok = ret.IsMoved(); ok {
			// Handle MOVED redirect - this requires primary connection swap
			return s.handleRedirectWithRetry(ctx, cmd, addr, result)
		} else if addr, ok = ret.IsAsk(); ok {
			// Handle ASK redirect - temporary redirect, no primary swap needed
			return s.handleAskRedirect(ctx, cmd, addr, result)
		}
	}

	return result
}

// handleRedirectWithRetry handles MOVED redirects with retry logic for repeated redirects
func (s *standalone) handleRedirectWithRetry(ctx context.Context, cmd Completed, addr string, originalResult ValkeyResult) ValkeyResult {
	for attempt := 0; attempt < s.retryLimit; attempt++ {
		// Update latest redirect address atomically
		s.latestRedirectAddr.Store(&addr)
		
		// Use singleflight to ensure only one redirect operation happens at a time
		// and only proceed if the address matches the latest seen redirect address
		if err := s.redirectCall.Do(ctx, func() error {
			// Double-check that the address still matches the latest redirect address
			// This guards against using stale addresses in concurrent scenarios
			if latest := s.latestRedirectAddr.Load(); latest == nil || *latest != addr {
				// Address has changed, abort this redirect
				return nil
			}
			return s.redirectToPrimary(addr)
		}); err != nil {
			// If redirect fails, return the original result
			return originalResult
		}

		// Execute the command on the updated primary
		result := s.primary.Do(ctx, cmd)
		
		// Check if we get another redirect error
		if result.Error() != nil {
			if ret, yes := IsValkeyErr(result.Error()); yes {
				if newAddr, ok := ret.IsMoved(); ok {
					// Another MOVED redirect - continue retry loop with new address
					addr = newAddr
					continue
				} else if askAddr, ok := ret.IsAsk(); ok {
					// ASK redirect after primary swap - handle it directly
					return s.handleAskRedirect(ctx, cmd, askAddr, result)
				}
			}
		}
		
		// No redirect error, return the result
		return result
	}
	
	// Exceeded retry limit, return original error
	return originalResult
}

// handleAskRedirect handles ASK redirects (temporary redirects that don't require primary swap)
func (s *standalone) handleAskRedirect(ctx context.Context, cmd Completed, addr string, originalResult ValkeyResult) ValkeyResult {
	// Create a temporary connection for ASK redirect
	redirectOpt := *s.opt
	redirectOpt.InitAddress = []string{addr}
	redirectConn := s.connFn(addr, &redirectOpt)
	if err := redirectConn.Dial(); err != nil {
		// If redirect fails, return the original result
		return originalResult
	}
	defer redirectConn.Close()

	// Create a temporary client for the ASK redirect
	redirectClient := newSingleClientWithConn(redirectConn, cmds.NewBuilder(cmds.NoSlot), !s.opt.DisableRetry, s.opt.DisableCache, s.retryer, false)
	defer redirectClient.Close()

	// Execute ASKING command followed by the original command
	results := redirectClient.DoMulti(ctx, cmds.AskingCmd, cmd)
	if len(results) >= 2 {
		return results[1] // Return result of the original command
	}
	
	// Fallback to original result if ASK redirect failed
	return originalResult
}

// redirectToPrimary swaps the primary connection to the specified redirect address
// This method should only be called from within singleflight coordination
func (s *standalone) redirectToPrimary(addr string) error {
	// Create a new connection to the redirect address
	redirectOpt := *s.opt
	redirectOpt.InitAddress = []string{addr}
	redirectConn := s.connFn(addr, &redirectOpt)
	if err := redirectConn.Dial(); err != nil {
		return err
	}

	// Create a new primary client with the redirect connection
	newPrimary := newSingleClientWithConn(redirectConn, cmds.NewBuilder(cmds.NoSlot), !s.opt.DisableRetry, s.opt.DisableCache, s.retryer, false)

	// Atomically swap the primary connection
	oldPrimary := s.primary
	s.primary = newPrimary
	
	// Close the old primary connection
	oldPrimary.Close()

	return nil
}

func (s *standalone) DoMulti(ctx context.Context, multi ...Completed) (resp []ValkeyResult) {
	toReplica := true
	if s.toReplicas != nil {
		for _, cmd := range multi {
			if !s.toReplicas(cmd) {
				toReplica = false
				break
			}
		}
	} else {
		toReplica = false
	}
	
	if toReplica {
		return s.replicas[s.pick()].DoMulti(ctx, multi...)
	}
	
	resp = s.primary.DoMulti(ctx, multi...)
	
	// Handle redirects if enabled and any command has a redirect error
	if s.enableRedirect && len(resp) > 0 {
		for _, result := range resp {
			if result.Error() != nil {
				if ret, yes := IsValkeyErr(result.Error()); yes {
					var addr string
					var ok bool
					
					if addr, ok = ret.IsMoved(); ok {
						// Handle MOVED redirect - retry the entire multi command
						return s.handleMultiRedirectWithRetry(ctx, multi, addr, resp)
					} else if addr, ok = ret.IsAsk(); ok {
						// Handle ASK redirect for this specific command
						// For multi commands, we need to retry the entire batch with ASK
						return s.handleAskMultiRedirect(ctx, multi, addr, resp)
					}
				}
			}
		}
	}
	
	return resp
}

// handleMultiRedirectWithRetry handles MOVED redirects for multi commands with retry logic
func (s *standalone) handleMultiRedirectWithRetry(ctx context.Context, multi []Completed, addr string, originalResp []ValkeyResult) []ValkeyResult {
	for attempt := 0; attempt < s.retryLimit; attempt++ {
		// Update latest redirect address atomically
		s.latestRedirectAddr.Store(&addr)
		
		// Use singleflight to ensure only one redirect operation happens at a time
		if err := s.redirectCall.Do(ctx, func() error {
			// Guard against stale addresses
			if latest := s.latestRedirectAddr.Load(); latest == nil || *latest != addr {
				return nil
			}
			return s.redirectToPrimary(addr)
		}); err != nil {
			// If redirect fails, return the original response
			return originalResp
		}

		// Execute the commands on the updated primary
		resp := s.primary.DoMulti(ctx, multi...)
		
		// Check if we get another redirect error
		if len(resp) > 0 {
			for _, result := range resp {
				if result.Error() != nil {
					if ret, yes := IsValkeyErr(result.Error()); yes {
						if newAddr, ok := ret.IsMoved(); ok {
							// Another MOVED redirect - continue retry loop
							addr = newAddr
							goto continue_retry
						} else if askAddr, ok := ret.IsAsk(); ok {
							// ASK redirect after primary swap
							return s.handleAskMultiRedirect(ctx, multi, askAddr, resp)
						}
					}
				}
			}
		}
		
		// No redirect error, return the response
		return resp
		
		continue_retry:
	}
	
	// Exceeded retry limit, return original response
	return originalResp
}

// handleAskMultiRedirect handles ASK redirects for multi commands
func (s *standalone) handleAskMultiRedirect(ctx context.Context, multi []Completed, addr string, originalResp []ValkeyResult) []ValkeyResult {
	// Create a temporary connection for ASK redirect
	redirectOpt := *s.opt
	redirectOpt.InitAddress = []string{addr}
	redirectConn := s.connFn(addr, &redirectOpt)
	if err := redirectConn.Dial(); err != nil {
		return originalResp
	}
	defer redirectConn.Close()

	// Create a temporary client for the ASK redirect
	redirectClient := newSingleClientWithConn(redirectConn, cmds.NewBuilder(cmds.NoSlot), !s.opt.DisableRetry, s.opt.DisableCache, s.retryer, false)
	defer redirectClient.Close()

	// Create commands with ASKING prefix
	askingMulti := make([]Completed, 0, len(multi)+1)
	askingMulti = append(askingMulti, cmds.AskingCmd)
	askingMulti = append(askingMulti, multi...)
	
	// Execute all commands together
	results := redirectClient.DoMulti(ctx, askingMulti...)
	if len(results) > 1 {
		// Return results excluding the ASKING command result
		return results[1:]
	}
	
	// Fallback to original response if ASK redirect failed
	return originalResp
}

func (s *standalone) Receive(ctx context.Context, subscribe Completed, fn func(msg PubSubMessage)) error {
	if s.toReplicas(subscribe) {
		return s.replicas[s.pick()].Receive(ctx, subscribe, fn)
	}
	return s.primary.Receive(ctx, subscribe, fn)
}

func (s *standalone) Close() {
	s.primary.Close()
	for _, replica := range s.replicas {
		replica.Close()
	}
}

func (s *standalone) DoCache(ctx context.Context, cmd Cacheable, ttl time.Duration) (resp ValkeyResult) {
	return s.primary.DoCache(ctx, cmd, ttl)
}

func (s *standalone) DoMultiCache(ctx context.Context, multi ...CacheableTTL) (resp []ValkeyResult) {
	return s.primary.DoMultiCache(ctx, multi...)
}

func (s *standalone) DoStream(ctx context.Context, cmd Completed) ValkeyResultStream {
	if s.toReplicas != nil && s.toReplicas(cmd) {
		return s.replicas[s.pick()].DoStream(ctx, cmd)
	}
	
	stream := s.primary.DoStream(ctx, cmd)
	
	// Handle redirects if enabled and there's an error
	if s.enableRedirect && stream.Error() != nil {
		if ret, yes := IsValkeyErr(stream.Error()); yes {
			var addr string
			var ok bool
			
			if addr, ok = ret.IsMoved(); ok {
				// Handle MOVED redirect with retry logic
				return s.handleStreamRedirectWithRetry(ctx, cmd, addr, stream)
			} else if addr, ok = ret.IsAsk(); ok {
				// Handle ASK redirect
				return s.handleAskStreamRedirect(ctx, cmd, addr, stream)
			}
		}
	}
	
	return stream
}

// handleStreamRedirectWithRetry handles MOVED redirects for streaming commands with retry logic
func (s *standalone) handleStreamRedirectWithRetry(ctx context.Context, cmd Completed, addr string, originalStream ValkeyResultStream) ValkeyResultStream {
	for attempt := 0; attempt < s.retryLimit; attempt++ {
		// Update latest redirect address atomically
		s.latestRedirectAddr.Store(&addr)
		
		// Use singleflight to ensure only one redirect operation happens at a time
		if err := s.redirectCall.Do(ctx, func() error {
			// Guard against stale addresses
			if latest := s.latestRedirectAddr.Load(); latest == nil || *latest != addr {
				return nil
			}
			return s.redirectToPrimary(addr)
		}); err != nil {
			// If redirect fails, return the original stream
			return originalStream
		}

		// Execute the command on the updated primary
		stream := s.primary.DoStream(ctx, cmd)
		
		// Check if we get another redirect error
		if stream.Error() != nil {
			if ret, yes := IsValkeyErr(stream.Error()); yes {
				if newAddr, ok := ret.IsMoved(); ok {
					// Another MOVED redirect - continue retry loop
					addr = newAddr
					continue
				} else if askAddr, ok := ret.IsAsk(); ok {
					// ASK redirect after primary swap
					return s.handleAskStreamRedirect(ctx, cmd, askAddr, stream)
				}
			}
		}
		
		// No redirect error, return the stream
		return stream
	}
	
	// Exceeded retry limit, return original stream
	return originalStream
}

// handleAskStreamRedirect handles ASK redirects for streaming commands
func (s *standalone) handleAskStreamRedirect(ctx context.Context, cmd Completed, addr string, originalStream ValkeyResultStream) ValkeyResultStream {
	// Create a temporary connection for ASK redirect
	redirectOpt := *s.opt
	redirectOpt.InitAddress = []string{addr}
	redirectConn := s.connFn(addr, &redirectOpt)
	if err := redirectConn.Dial(); err != nil {
		return originalStream
	}
	defer redirectConn.Close()

	// Create a temporary client for the ASK redirect
	redirectClient := newSingleClientWithConn(redirectConn, cmds.NewBuilder(cmds.NoSlot), !s.opt.DisableRetry, s.opt.DisableCache, s.retryer, false)
	defer redirectClient.Close()

	// Execute ASKING command first, then the stream command
	if result := redirectClient.Do(ctx, cmds.AskingCmd); result.Error() != nil {
		return originalStream
	}
	
	// Execute the original streaming command
	return redirectClient.DoStream(ctx, cmd)
}

func (s *standalone) DoMultiStream(ctx context.Context, multi ...Completed) MultiValkeyResultStream {
	toReplica := true
	if s.toReplicas != nil {
		for _, cmd := range multi {
			if !s.toReplicas(cmd) {
				toReplica = false
				break
			}
		}
	} else {
		toReplica = false
	}
	
	if toReplica {
		return s.replicas[s.pick()].DoMultiStream(ctx, multi...)
	}
	
	stream := s.primary.DoMultiStream(ctx, multi...)
	
	// Handle redirects if enabled and there's an error
	if s.enableRedirect && stream.Error() != nil {
		if ret, yes := IsValkeyErr(stream.Error()); yes {
			var addr string
			var ok bool
			
			if addr, ok = ret.IsMoved(); ok {
				// Handle MOVED redirect with retry logic
				return s.handleMultiStreamRedirectWithRetry(ctx, multi, addr, stream)
			} else if addr, ok = ret.IsAsk(); ok {
				// Handle ASK redirect
				return s.handleAskMultiStreamRedirect(ctx, multi, addr, stream)
			}
		}
	}
	
	return stream
}

// handleMultiStreamRedirectWithRetry handles MOVED redirects for multi-streaming commands with retry logic
func (s *standalone) handleMultiStreamRedirectWithRetry(ctx context.Context, multi []Completed, addr string, originalStream MultiValkeyResultStream) MultiValkeyResultStream {
	for attempt := 0; attempt < s.retryLimit; attempt++ {
		// Update latest redirect address atomically
		s.latestRedirectAddr.Store(&addr)
		
		// Use singleflight to ensure only one redirect operation happens at a time
		if err := s.redirectCall.Do(ctx, func() error {
			// Guard against stale addresses
			if latest := s.latestRedirectAddr.Load(); latest == nil || *latest != addr {
				return nil
			}
			return s.redirectToPrimary(addr)
		}); err != nil {
			// If redirect fails, return the original stream
			return originalStream
		}

		// Execute the commands on the updated primary
		stream := s.primary.DoMultiStream(ctx, multi...)
		
		// Check if we get another redirect error
		if stream.Error() != nil {
			if ret, yes := IsValkeyErr(stream.Error()); yes {
				if newAddr, ok := ret.IsMoved(); ok {
					// Another MOVED redirect - continue retry loop
					addr = newAddr
					continue
				} else if askAddr, ok := ret.IsAsk(); ok {
					// ASK redirect after primary swap
					return s.handleAskMultiStreamRedirect(ctx, multi, askAddr, stream)
				}
			}
		}
		
		// No redirect error, return the stream
		return stream
	}
	
	// Exceeded retry limit, return original stream
	return originalStream
}

// handleAskMultiStreamRedirect handles ASK redirects for multi-streaming commands
func (s *standalone) handleAskMultiStreamRedirect(ctx context.Context, multi []Completed, addr string, originalStream MultiValkeyResultStream) MultiValkeyResultStream {
	// Create a temporary connection for ASK redirect
	redirectOpt := *s.opt
	redirectOpt.InitAddress = []string{addr}
	redirectConn := s.connFn(addr, &redirectOpt)
	if err := redirectConn.Dial(); err != nil {
		return originalStream
	}
	defer redirectConn.Close()

	// Create a temporary client for the ASK redirect
	redirectClient := newSingleClientWithConn(redirectConn, cmds.NewBuilder(cmds.NoSlot), !s.opt.DisableRetry, s.opt.DisableCache, s.retryer, false)
	defer redirectClient.Close()

	// Execute ASKING command first, then the multi-stream commands
	if result := redirectClient.Do(ctx, cmds.AskingCmd); result.Error() != nil {
		return originalStream
	}
	
	// Execute the original multi-streaming commands
	return redirectClient.DoMultiStream(ctx, multi...)
}

func (s *standalone) Dedicated(fn func(DedicatedClient) error) (err error) {
	return s.primary.Dedicated(fn)
}

func (s *standalone) Dedicate() (client DedicatedClient, cancel func()) {
	return s.primary.Dedicate()
}

func (s *standalone) Nodes() map[string]Client {
	nodes := make(map[string]Client, len(s.replicas)+1)
	for addr, client := range s.primary.Nodes() {
		nodes[addr] = client
	}
	for _, replica := range s.replicas {
		for addr, client := range replica.Nodes() {
			nodes[addr] = client
		}
	}
	return nodes
}

func (s *standalone) Mode() ClientMode {
	return ClientModeStandalone
}
