package valkey

import (
	"context"
	"io"
	"strings"
	"sync/atomic"
	"time"

	"github.com/valkey-io/valkey-go/internal/cmds"
)

type singleClient struct {
	conn           conn
	retryHandler   retryHandler
	enableRedirect bool
	stop           uint32
	cmd            Builder
	retry          bool
	hasLftm        bool
	DisableCache   bool

	connFn connFn        // NEW: store connFn so we can reconnect
	opt    *ClientOption // NEW: store opt so we can reuse connection options
}

func newSingleClient(opt *ClientOption, prev conn, connFn connFn, retryer retryHandler) (*singleClient, error) {
	if len(opt.InitAddress) == 0 {
		return nil, ErrNoAddr
	}

	if opt.ReplicaOnly {
		return nil, ErrReplicaOnlyNotSupported
	}

	conn := connFn(opt.InitAddress[0], opt)
	conn.Override(prev)
	if err := conn.Dial(); err != nil {
		return nil, err
	}
	return newSingleClientWithConn(conn, cmds.NewBuilder(cmds.NoSlot), !opt.DisableRetry, opt.DisableCache, retryer, opt.ConnLifetime > 0), nil
}

func newSingleClientWithConn(conn conn, builder Builder, retry, disableCache bool, retryer retryHandler, hasLftm bool) *singleClient {
	return &singleClient{cmd: builder, conn: conn, retry: retry, retryHandler: retryer, hasLftm: hasLftm, DisableCache: disableCache}
}

func (c *singleClient) B() Builder {
	return c.cmd
}

func (c *singleClient) Do(ctx context.Context, cmd Completed) (resp ValkeyResult) {
	attempts := 1
retry:
	resp = c.conn.Do(ctx, cmd)
	if err := resp.Error(); err != nil {
		if err == errConnExpired {
			goto retry
		}
		if isRedirectError(err) {
			// parse host:port from error
			host, port := parseRedirectTarget(err.Error())
			if host != "" && port != "" {
				if c.reconnectTo(host, port) == nil {
					goto retry
				}
			}
			// fallthrough — return redirect error if reconnect failed
		}
		if c.retry && cmd.IsReadOnly() && c.isRetryable(err, ctx) {
			if c.retryHandler.WaitOrSkipRetry(ctx, attempts, cmd, err) {
				attempts++
				goto retry
			}
		}
	}
	if resp.NonValkeyError() == nil {
		cmds.PutCompleted(cmd)
	}
	return resp
}

func (c *singleClient) DoStream(ctx context.Context, cmd Completed) ValkeyResultStream {
	s := c.conn.DoStream(ctx, cmd)
	cmds.PutCompleted(cmd)
	return s
}

func (c *singleClient) DoMultiStream(ctx context.Context, multi ...Completed) MultiValkeyResultStream {
	if len(multi) == 0 {
		return ValkeyResultStream{e: io.EOF}
	}
	s := c.conn.DoMultiStream(ctx, multi...)
	for _, cmd := range multi {
		cmds.PutCompleted(cmd)
	}
	return s
}

func (c *singleClient) DoMulti(ctx context.Context, multi ...Completed) (resps []ValkeyResult) {
	if len(multi) == 0 {
		return nil
	}
	attempts := 1
retry:
	resps = c.conn.DoMulti(ctx, multi...).s
	if c.hasLftm {
		var ml []Completed
	recover:
		ml = ml[:0]
		var txIdx int // check transaction block, if zero, then not in transaction
		for i, resp := range resps {
			if resp.NonValkeyError() == errConnExpired {
				if txIdx > 0 {
					ml = multi[txIdx:]
				} else {
					ml = multi[i:]
				}
				break
			}
			// if no error, then check if transaction block
			if isMulti(multi[i]) {
				txIdx = i
			} else if isExec(multi[i]) {
				txIdx = 0
			}
		}
		if len(ml) > 0 {
			rs := c.conn.DoMulti(ctx, ml...).s
			resps = append(resps[:len(resps)-len(rs)], rs...)
			goto recover
		}
	}
	if c.retry && allReadOnly(multi) {
		for i, resp := range resps {
			if c.isRetryable(resp.Error(), ctx) {
				shouldRetry := c.retryHandler.WaitOrSkipRetry(
					ctx, attempts, multi[i], resp.Error(),
				)
				if shouldRetry {
					attempts++
					goto retry
				}
			}
		}
	}
	for i, cmd := range multi {
		if resps[i].NonValkeyError() == nil {
			cmds.PutCompleted(cmd)
		}
	}
	return resps
}

func (c *singleClient) DoMultiCache(ctx context.Context, multi ...CacheableTTL) (resps []ValkeyResult) {
	if len(multi) == 0 {
		return nil
	}
	attempts := 1
retry:
	resps = c.conn.DoMultiCache(ctx, multi...).s
	if c.hasLftm {
		var ml []CacheableTTL
	recover:
		ml = ml[:0]
		for i, resp := range resps {
			if resp.NonValkeyError() == errConnExpired {
				ml = multi[i:]
				break
			}
		}
		if len(ml) > 0 {
			rs := c.conn.DoMultiCache(ctx, ml...).s
			resps = append(resps[:len(resps)-len(rs)], rs...)
			goto recover
		}
	}
	if c.retry {
		for i, resp := range resps {
			if c.isRetryable(resp.Error(), ctx) {
				shouldRetry := c.retryHandler.WaitOrSkipRetry(
					ctx, attempts, Completed(multi[i].Cmd), resp.Error(),
				)
				if shouldRetry {
					attempts++
					goto retry
				}
			}
		}
	}
	for i, cmd := range multi {
		if err := resps[i].NonValkeyError(); err == nil || err == ErrDoCacheAborted {
			cmds.PutCacheable(cmd.Cmd)
		}
	}
	return resps
}

func (c *singleClient) DoCache(ctx context.Context, cmd Cacheable, ttl time.Duration) (resp ValkeyResult) {
	attempts := 1
retry:
	resp = c.conn.DoCache(ctx, cmd, ttl)
	if err := resp.Error(); err != nil {
		if err == errConnExpired {
			goto retry
		}
		if c.retry && c.isRetryable(err, ctx) {
			if c.retryHandler.WaitOrSkipRetry(ctx, attempts, Completed(cmd), err) {
				attempts++
				goto retry
			}
		}
	}
	if err := resp.NonValkeyError(); err == nil || err == ErrDoCacheAborted {
		cmds.PutCacheable(cmd)
	}
	return resp
}

func (c *singleClient) Receive(ctx context.Context, subscribe Completed, fn func(msg PubSubMessage)) (err error) {
	attempts := 1
retry:
	err = c.conn.Receive(ctx, subscribe, fn)
	if err == errConnExpired {
		goto retry
	}
	if c.retry {
		if _, ok := err.(*ValkeyError); !ok && c.isRetryable(err, ctx) {
			shouldRetry := c.retryHandler.WaitOrSkipRetry(ctx, attempts, subscribe, err)
			if shouldRetry {
				attempts++
				goto retry
			}
		}
	}
	if err == nil {
		cmds.PutCompleted(subscribe)
	}
	return err
}

func (c *singleClient) Dedicated(fn func(DedicatedClient) error) (err error) {
	wire := c.conn.Acquire(context.Background())
	dsc := &dedicatedSingleClient{cmd: c.cmd, conn: c.conn, wire: wire, retry: c.retry, retryHandler: c.retryHandler}
	err = fn(dsc)
	dsc.release()
	return err
}

func (c *singleClient) Dedicate() (DedicatedClient, func()) {
	wire := c.conn.Acquire(context.Background())
	dsc := &dedicatedSingleClient{cmd: c.cmd, conn: c.conn, wire: wire, retry: c.retry, retryHandler: c.retryHandler}
	return dsc, dsc.release
}

func (c *singleClient) Nodes() map[string]Client {
	return map[string]Client{c.conn.Addr(): c}
}

func (c *singleClient) Mode() ClientMode {
	return ClientModeStandalone
}

func (c *singleClient) Close() {
	atomic.StoreUint32(&c.stop, 1)
	c.conn.Close()
}

type dedicatedSingleClient struct {
	conn         conn
	wire         wire
	retryHandler retryHandler
	mark         uint32
	cmd          Builder
	retry        bool
}

func (c *dedicatedSingleClient) B() Builder {
	return c.cmd
}

func (c *dedicatedSingleClient) Do(ctx context.Context, cmd Completed) (resp ValkeyResult) {
	attempts := 1
retry:
	if err := c.check(); err != nil {
		return newErrResult(err)
	}
	resp = c.wire.Do(ctx, cmd)
	if c.retry && cmd.IsReadOnly() && isRetryable(resp.Error(), c.wire, ctx) {
		shouldRetry := c.retryHandler.WaitOrSkipRetry(
			ctx, attempts, cmd, resp.Error(),
		)
		if shouldRetry {
			attempts++
			goto retry
		}
	}
	if resp.NonValkeyError() == nil {
		cmds.PutCompleted(cmd)
	}
	return resp
}

func (c *dedicatedSingleClient) DoMulti(ctx context.Context, multi ...Completed) (resp []ValkeyResult) {
	if len(multi) == 0 {
		return nil
	}
	attempts := 1
	retryable := c.retry
	if retryable {
		retryable = allReadOnly(multi)
	}
retry:
	if err := c.check(); err != nil {
		return fillErrs(len(multi), err)
	}
	resp = c.wire.DoMulti(ctx, multi...).s
	for i, cmd := range multi {
		if retryable && isRetryable(resp[i].Error(), c.wire, ctx) {
			shouldRetry := c.retryHandler.WaitOrSkipRetry(
				ctx, attempts, multi[i], resp[i].Error(),
			)
			if shouldRetry {
				attempts++
				goto retry
			}
		}
		if resp[i].NonValkeyError() == nil {
			cmds.PutCompleted(cmd)
		}
	}
	return resp
}

func (c *dedicatedSingleClient) Receive(ctx context.Context, subscribe Completed, fn func(msg PubSubMessage)) (err error) {
	attempts := 1
retry:
	if err := c.check(); err != nil {
		return err
	}
	err = c.wire.Receive(ctx, subscribe, fn)
	if c.retry {
		if _, ok := err.(*ValkeyError); !ok && isRetryable(err, c.wire, ctx) {
			shouldRetry := c.retryHandler.WaitOrSkipRetry(
				ctx, attempts, subscribe, err,
			)
			if shouldRetry {
				attempts++
				goto retry
			}
		}
	}
	if err == nil {
		cmds.PutCompleted(subscribe)
	}
	return err
}

func (c *dedicatedSingleClient) SetPubSubHooks(hooks PubSubHooks) <-chan error {
	if err := c.check(); err != nil {
		ch := make(chan error, 1)
		ch <- err
		return ch
	}
	return c.wire.SetPubSubHooks(hooks)
}

func (c *dedicatedSingleClient) Close() {
	c.wire.Close()
	c.release()
}

func (c *dedicatedSingleClient) check() error {
	if atomic.LoadUint32(&c.mark) != 0 {
		return ErrDedicatedClientRecycled
	}
	return nil
}

func (c *dedicatedSingleClient) release() {
	if atomic.CompareAndSwapUint32(&c.mark, 0, 1) {
		c.conn.Store(c.wire)
	}
}

func (c *singleClient) isRetryable(err error, ctx context.Context) bool {
	if err == nil || err == Nil || err == ErrDoCacheAborted || atomic.LoadUint32(&c.stop) != 0 || ctx.Err() != nil {
		return false
	}
	if err, ok := err.(*ValkeyError); ok {
		return err.IsLoading()
	}
	return true
}

func isRetryable(err error, w wire, ctx context.Context) bool {
	if err == nil || err == Nil || w.Error() != nil || ctx.Err() != nil {
		return false
	}
	if err, ok := err.(*ValkeyError); ok {
		return err.IsLoading()
	}
	return true
}

func allReadOnly(multi []Completed) bool {
	for _, cmd := range multi {
		if cmd.IsWrite() {
			return false
		}
	}
	return true
}

func chooseSlot(multi []Completed) uint16 {
	for i := 0; i < len(multi); i++ {
		if multi[i].Slot() != cmds.InitSlot {
			for j := i + 1; j < len(multi); j++ {
				if multi[j].Slot() != cmds.InitSlot && multi[j].Slot() != multi[i].Slot() {
					return cmds.NoSlot
				}
			}
			return multi[i].Slot()
		}
	}
	return cmds.InitSlot
}

func isRedirectError(err error) bool {
	return strings.HasPrefix(err.Error(), "REDIRECT ")
}

func parseRedirectTarget(msg string) (string, string) {
	parts := strings.Fields(msg)
	if len(parts) < 2 {
		return "", ""
	}
	hostPort := strings.Split(parts[1], ":")
	if len(hostPort) != 2 {
		return "", ""
	}
	return hostPort[0], hostPort[1]
}

func (s *singleClient) reconnectTo(host, port string) error {
	s.conn.Close()
	newConn := s.connFn(host+":"+port, s.opt) // you'd store connFn and opt in singleClient
	if err := newConn.Dial(); err != nil {
		return err
	}
	if s.enableRedirect { // you'd add enableRedirect field to singleClient
		if err := sendClientCapa(newConn); err != nil {
			newConn.Close()
			return err
		}
	}
	s.conn = newConn
	return nil
}
