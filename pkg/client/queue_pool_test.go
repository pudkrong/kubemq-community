package client

import (
	"context"
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
	"github.com/stretchr/testify/require"
)

// TestQueuePool_GetClient_RebuildsDeadClient guards Fix D: when a pooled
// client's STAN connection dies (isUp=false) and it is no longer in use, the
// next GetClient must replace it with a fresh, healthy client instead of
// handing back the poisoned one (which previously caused every poll on that
// channel to fail with Error 121/302 until the watcher reaped it ~60s later).
func TestQueuePool_GetClient_RebuildsDeadClient(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := getConfig(t)
	s1 := setupSingle(ctx, t, cfg)
	defer tearDownSingle(s1, cfg)
	time.Sleep(1 * time.Second)

	pool := NewQueuePool(ctx, &QueuePoolOptions{
		KillAfter: 1 * time.Minute, // long enough that the watcher never ticks during the test
		MaxUsage:  10,
	}, cfg)
	defer pool.Close()

	channel := "test-pool-channel-dead-rebuild"

	// First acquire creates and stores pool client P1.
	c1, err := pool.GetClient(channel)
	require.NoError(t, err)
	require.True(t, c1.isUp.Load(), "newly created pool client must be up")

	// Simulate the STAN connection dying while the client is in use
	// (equivalent to ConnectionLostHandler firing -> isUp=false).
	c1.isUp.Store(false)
	require.False(t, c1.isUp.Load(), "client should be down after simulated connection loss")

	// While still "in use" (usedCounter>0) the dead client must NOT be rebuilt:
	// evicting it would orphan an in-flight transaction.
	c1Again, err := pool.GetClient(channel)
	require.NoError(t, err)
	require.Same(t, c1, c1Again, "in-use dead client should be returned as-is, not rebuilt")
	pool.ReleaseClient(channel) // balance the extra acquire (c1Again)

	// Release the original acquire so the client becomes idle (usedCounter==0).
	pool.ReleaseClient(channel)

	// Dead AND idle -> the next acquire must rebuild and return a fresh client.
	c2, err := pool.GetClient(channel)
	require.NoError(t, err)
	require.True(t, c2.isUp.Load(), "rebuilt pool client must be up")
	require.NotSame(t, c1, c2, "GetClient must return a NEW client after the old one died")

	// Cleanup: release so pool.Close() can tear the rebuilt client down.
	pool.ReleaseClient(channel)
}
