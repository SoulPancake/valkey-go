# valkeylock

A [Valkey Distributed Lock Pattern](https://redis.io/docs/latest/develop/use/patterns/distributed-locks/) enhanced by [Client Side Caching](https://redis.io/docs/manual/client-side-caching/).

```go
package main

import (
	"context"
	"github.com/valkey-io/valkey-go"
	"github.com/valkey-io/valkey-go/valkeylock"
)

func main() {
	locker, err := valkeylock.NewLocker(valkeylock.LockerOption{
		ClientOption:   valkey.ClientOption{InitAddress: []string{"localhost:6379"}},
		KeyMajority:    1,    // Use KeyMajority=1 if you have only one Valkey instance. Also make sure that all your `Locker`s share the same KeyMajority.
		NoLoopTracking: true, // Enable this to have better performance if all your Valkey are >= 7.0.5.
	})
	if err != nil {
		panic(err)
	}
	defer locker.Close()

	// acquire the lock "my_lock"
	ctx, cancel, err := locker.WithContext(context.Background(), "my_lock")
	if err != nil {
		panic(err)
	}

	// "my_lock" is acquired. use the ctx as normal.
	doSomething(ctx)

	// invoke cancel() to release the lock.
	cancel()
}
```

## Default Configuration

When creating a new `Locker` without explicitly setting options, the following defaults are applied:

- **KeyPrefix**: `"valkeylock"`  
- **KeyValidity**: `5s` — lock validity period before it expires.  
- **ExtendInterval**: `KeyValidity / 2` (default: 2.5s) — how often the lock is automatically extended.  
- **TryNextAfter**: `20ms` — wait time before trying the next Valkey key when acquiring locks.  
- **KeyMajority**: `2` — number of keys required out of `KeyMajority*2-1` total keys for a valid lock.  
- **NoLoopTracking**: `false` — disables NOLOOP in client tracking unless explicitly set.  
- **FallbackSETPX**: `false` — uses `SET PXAT` by default; set to `true` for Redis versions < 6.2.

These values can be overridden by providing a `LockerOption` when calling `NewLocker`.

## Features backed by the Valkey Client Side Caching
* The returned `ctx` will be canceled automatically and immediately once the `KeyMajority` is not held anymore, for example:
  * Valkey are down.
  * Acquired keys have been deleted by other programs or administrators.
* The waiting `Locker.WithContext` will try acquiring the lock again automatically and immediately once it has been released by someone or by another program.

## How it works

When the `locker.WithContext` is invoked, it will:

1. Try acquiring 3 keys (given that the default `KeyMajority` is 2), which are `valkeylock:0:my_lock`, `valkeylock:1:my_lock` and `valkeylock:2:my_lock`, by sending valkey command `SET NX PXAT` or `SET NX PX` if `FallbackSETPX` is set.
2. If the `KeyMajority` is satisfied within the `KeyValidity` duration, the invocation is successful and a `ctx` is returned as the lock.
3. If the invocation is not successful, it will wait for client-side caching notifications to retry again.
4. If the invocation is successful, the `Locker` will extend the `ctx` validity periodically and also watch client-side caching notifications for canceling the `ctx` if the `KeyMajority` is not held anymore.

### Disable Client Side Caching

Some Valkey providers don't support client-side caching, ex. Google Cloud Memorystore.
You can disable client-side caching by setting `ClientOption.DisableCache` to `true`.
Please note that when the client-side caching is disabled, valkeylock will only try to re-acquire locks for every ExtendInterval.

## Benchmark

```bash
▶ go test -bench=. -benchmem -run=.
goos: darwin
goarch: arm64
pkg: valkey-benchmark/locker
Benchmark/valkeylock-10         	   20103	     57842 ns/op	    1849 B/op	      29 allocs/op
Benchmark/redislock-10           	   13209	     86285 ns/op	    8083 B/op	     225 allocs/op
PASS
ok  	valkey-benchmark/locker	3.782s
```

```go
package locker

import (
	"context"
	"testing"
	"time"

	"github.com/bsm/redislock"
	"github.com/redis/go-redis/v9"
	"github.com/valkey-io/valkey-go"
	"github.com/valkey-io/valkey-go/valkeylock"
)

func Benchmark(b *testing.B) {
	b.Run("valkeylock", func(b *testing.B) {
		l, _ := valkeylock.NewLocker(valkeylock.LockerOption{
			ClientOption:   valkey.ClientOption{InitAddress: []string{"127.0.0.1:6379"}},
			KeyMajority:    1,
			NoLoopTracking: true,
		})
		defer l.Close()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				_, cancel, err := l.WithContext(context.Background(), "mylock")
				if err != nil {
					panic(err)
				}
				cancel()
			}
		})
		b.StopTimer()
	})
	b.Run("redislock", func(b *testing.B) {
		client := redis.NewUniversalClient(&redis.UniversalOptions{Addrs: []string{"127.0.0.1:6379"}})
		locker := redislock.New(client)
		defer client.Close()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
			retry:
				lock, err := locker.Obtain(context.Background(), "mylock", time.Minute, nil)
				if err == redislock.ErrNotObtained {
					goto retry
				} else if err != nil {
					panic(err)
				}
				lock.Release(context.Background())
			}
		})
		b.StopTimer()
	})
}
```
