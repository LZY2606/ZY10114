package ttlcache

import (
	"context"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func waitForChannel(t *testing.T, ch <-chan struct{}, message string) {
	t.Helper()

	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal(message)
	}
}

func drainTimerCh(c *Cache[string, string]) {
	select {
	case <-c.items.timerCh:
	default:
	}
}

func assertLRUOrder(t *testing.T, c *Cache[string, string], want ...string) {
	t.Helper()

	var got []string
	for elem := c.items.lru.Front(); elem != nil; elem = elem.Next() {
		got = append(got, elem.Value.(*Item[string, string]).key)
	}
	assert.Equal(t, want, got)
}

func assertHeapOrder(t *testing.T, c *Cache[string, string], want ...string) {
	t.Helper()

	var got []string
	for _, elem := range c.items.expQueue {
		got = append(got, elem.Value.(*Item[string, string]).key)
	}
	assert.Equal(t, want, got)
}

func assertHeapIndices(t *testing.T, c *Cache[string, string]) {
	t.Helper()

	require.Len(t, c.items.values, c.items.expQueue.Len())
	require.Equal(t, c.items.lru.Len(), c.items.expQueue.Len())

	for i, elem := range c.items.expQueue {
		item := elem.Value.(*Item[string, string])
		require.Contains(t, c.items.values, item.key)
		assert.Same(t, c.items.values[item.key], elem)
		assert.Equal(t, i, item.queueIndex)
	}
}

func assertTimerNotification(t *testing.T, c *Cache[string, string], want time.Duration) {
	t.Helper()

	select {
	case got := <-c.items.timerCh:
		assert.InDelta(t, want, got, float64(time.Second))
	default:
		t.Fatal("expected timer notification")
	}
}

func assertNoTimerNotification(t *testing.T, c *Cache[string, string]) {
	t.Helper()

	select {
	case got := <-c.items.timerCh:
		t.Fatalf("unexpected timer notification: %v", got)
	default:
	}
}

func addInternalExpiredItem(c *Cache[string, string], key string) *Item[string, string] {
	item := NewItemWithOpts(key, "v", time.Nanosecond, c.options.itemOpts...)
	item.expiresAt = time.Now().Add(-time.Nanosecond)
	elem := c.items.lru.PushFront(item)
	c.items.values[key] = elem
	c.items.expQueue.push(elem)
	if c.options.maxCost != 0 {
		c.cost += item.cost
	}
	return item
}

func TestConsistency_SetReplacementAndFreshPath(t *testing.T) {
	c := prepCache(0, 0)

	old := c.Set("k", "old", NoTTL)
	require.Len(t, c.items.expQueue, 1)
	assert.Same(t, old, c.items.values["k"].Value)
	assert.Equal(t, 0, old.queueIndex)
	assertLRUOrder(t, c, "k")
	assertHeapOrder(t, c, "k")
	assertHeapIndices(t, c)
	assertNoTimerNotification(t, c)

	updated := c.Set("k", "new", time.Hour)
	assert.Same(t, old, updated)
	assert.Equal(t, "new", updated.value)
	assert.Equal(t, time.Hour, updated.ttl)
	assert.Equal(t, 0, updated.queueIndex)
	assertLRUOrder(t, c, "k")
	assertHeapOrder(t, c, "k")
	assertHeapIndices(t, c)
	assertTimerNotification(t, c, time.Hour)
}

func TestConsistency_TouchAndNoTouchPaths(t *testing.T) {
	c := prepCache(0, 0)
	original := c.Set("a", "av", time.Hour)
	other := c.Set("b", "bv", 2*time.Hour)
	drainTimerCh(c)
	originalExpiresAt := original.expiresAt

	require.Equal(t, 0, original.queueIndex)
	require.Equal(t, 1, other.queueIndex)
	assertLRUOrder(t, c, "b", "a")
	assertHeapOrder(t, c, "a", "b")

	c.Get("a")
	assert.True(t, original.expiresAt.After(originalExpiresAt))
	assert.Equal(t, 0, original.queueIndex)
	assert.Equal(t, 1, other.queueIndex)
	assertLRUOrder(t, c, "a", "b")
	assertHeapOrder(t, c, "a", "b")
	assertHeapIndices(t, c)
	assertNoTimerNotification(t, c)

	fixedExpiresAt := time.Now().Add(30 * time.Minute)
	original.expiresAt = fixedExpiresAt
	c.items.expQueue.update(c.items.values["a"])
	drainTimerCh(c)

	c.Get("a", WithDisableTouchOnHit[string, string]())
	assert.Equal(t, fixedExpiresAt, original.expiresAt)
	assertLRUOrder(t, c, "a", "b")
	assertHeapOrder(t, c, "a", "b")
	assertHeapIndices(t, c)
	assertNoTimerNotification(t, c)
}

func TestConsistency_UpdateTTLNotifications(t *testing.T) {
	c := prepCache(0, 0)
	a := c.Set("a", "v", time.Hour)
	b := c.Set("b", "v", 2*time.Hour)
	drainTimerCh(c)

	updatedA := c.Set("a", "v", 3*time.Hour)
	assert.Same(t, a, updatedA)
	assertHeapOrder(t, c, "b", "a")
	assertHeapIndices(t, c)
	assertNoTimerNotification(t, c)

	updatedB := c.Set("b", "v", time.Minute)
	assert.Same(t, b, updatedB)
	assertHeapOrder(t, c, "b", "a")
	assertHeapIndices(t, c)
	assertTimerNotification(t, c, time.Minute)
}

func TestConsistency_DeleteExpiredPath(t *testing.T) {
	c := prepCache(0, time.Hour)
	expired := addInternalExpiredItem(c, "expired")
	live := c.Set("live", "v", time.Hour)
	require.Same(t, expired, c.items.values["expired"].Value)
	require.Equal(t, 0, expired.queueIndex)
	require.Equal(t, 1, live.queueIndex)
	assertHeapOrder(t, c, "expired", "live")
	assertLRUOrder(t, c, "live", "expired")

	c.DeleteExpired()
	assert.NotContains(t, c.items.values, "expired")
	assert.Equal(t, -1, expired.queueIndex)
	assert.Same(t, live, c.items.values["live"].Value)
	assert.Equal(t, 0, live.queueIndex)
	assertLRUOrder(t, c, "live")
	assertHeapOrder(t, c, "live")
	assertHeapIndices(t, c)
}

func TestConsistency_CapacityEvictionPath(t *testing.T) {
	c := New[string, string](
		WithTTL[string, string](time.Hour),
		WithCapacity[string, string](2),
	)
	oldest := c.Set("a", "v", NoTTL)
	middle := c.Set("b", "v", time.Hour)
	drainTimerCh(c)

	assertLRUOrder(t, c, "b", "a")
	assertHeapOrder(t, c, "b", "a")
	assertHeapIndices(t, c)

	evicted := make(chan *Item[string, string], 1)
	c.OnEviction(func(_ context.Context, reason EvictionReason, item *Item[string, string]) {
		if reason == EvictionReasonCapacityReached {
			evicted <- item
		}
	})

	newest := c.Set("c", "v", 2*time.Hour)
	assert.Same(t, oldest, <-evicted)
	assert.NotContains(t, c.items.values, "a")
	assert.Equal(t, -1, oldest.queueIndex)
	assert.Same(t, middle, c.items.values["b"].Value)
	assert.Same(t, newest, c.items.values["c"].Value)
	assert.Equal(t, 0, middle.queueIndex)
	assert.Equal(t, 1, newest.queueIndex)
	assertLRUOrder(t, c, "c", "b")
	assertHeapOrder(t, c, "b", "c")
	assertHeapIndices(t, c)
}

func TestConsistency_CostEvictionPath(t *testing.T) {
	c := New[string, string](
		WithMaxCost[string, string](3, func(item CostItem[string, string]) uint64 {
			return uint64(len(item.Value))
		}),
	)
	oldest := c.Set("a", "xx", NoTTL)
	middle := c.Set("b", "x", NoTTL)
	require.EqualValues(t, 3, c.Cost())

	newest := c.Set("c", "xx", 2*time.Hour)
	assert.NotContains(t, c.items.values, "a")
	assert.Equal(t, -1, oldest.queueIndex)
	assert.Contains(t, c.items.values, "b")
	assert.Contains(t, c.items.values, "c")
	assert.EqualValues(t, 3, c.Cost())
	assert.Same(t, middle, c.items.values["b"].Value)
	assert.Same(t, newest, c.items.values["c"].Value)
	assertLRUOrder(t, c, "c", "b")
	assertHeapOrder(t, c, "c", "b")
	assertHeapIndices(t, c)
}

func TestConsistency_TTLBoundaries(t *testing.T) {
	c := New[string, string](WithTTL[string, string](time.Hour))
	zero := c.Set("zero", "v", DefaultTTL)
	assert.Equal(t, time.Hour, zero.ttl)
	assert.False(t, zero.expiresAt.IsZero())
	assert.Equal(t, 0, zero.queueIndex)

	negative := c.Set("negative", "v", time.Duration(-3))
	assert.Equal(t, time.Duration(-3), negative.ttl)
	assert.True(t, negative.expiresAt.IsZero())
	assert.False(t, negative.IsExpired())
	assert.GreaterOrEqual(t, negative.queueIndex, 0)

	expired := addInternalExpiredItem(c, "pending")
	assert.True(t, expired.IsExpired())
	assert.Contains(t, c.items.values, "pending")
	assert.Nil(t, c.Get("pending"))
	assert.False(t, c.Has("pending"))
	assert.Contains(t, c.items.values, "pending")
	assert.Equal(t, 3, c.items.expQueue.Len())

	revived := c.Set("pending", "v", NoTTL)
	assert.Same(t, expired, revived)
	assert.False(t, revived.IsExpired())
	assert.GreaterOrEqual(t, revived.queueIndex, 0)
	assertHeapIndices(t, c)
	assert.NotNil(t, c.Get("pending", WithDisableTouchOnHit[string, string]()))
}

func TestConsistency_BackgroundCleanupViaBarrierWithoutSleep(t *testing.T) {
	c := New[string, string](WithTTL[string, string](time.Hour))
	expired := addInternalExpiredItem(c, "expired")
	c.items.timerCh <- 0

	removed := make(chan struct{})
	c.OnEviction(func(_ context.Context, reason EvictionReason, item *Item[string, string]) {
		if reason == EvictionReasonExpired && item.Key() == "expired" {
			close(removed)
		}
	})

	done := make(chan struct{})
	go func() {
		c.Start()
		close(done)
	}()

	waitForChannel(t, removed, "background cleaner did not remove queued expired item")
	c.Stop()
	waitForChannel(t, done, "Start did not return after Stop")
	assert.NotContains(t, c.items.values, "expired")
	assert.Equal(t, -1, expired.queueIndex)
}

func TestConsistency_StopDoesNotSweep(t *testing.T) {
	c := New[string, string](WithTTL[string, string](time.Hour))
	started := make(chan struct{})
	go func() {
		close(started)
		c.Start()
	}()

	waitForChannel(t, started, "Start goroutine did not begin")
	deadline := time.Now().Add(time.Second)
	for !c.IsStarted() {
		if time.Now().After(deadline) {
			t.Fatal("Start did not become active")
		}
		runtime.Gosched()
	}

	c.Stop()
	assert.False(t, c.IsStarted())
	pending := addInternalExpiredItem(c, "pending")
	assert.Contains(t, c.items.values, "pending")
	assert.Equal(t, 0, pending.queueIndex)
	assert.True(t, pending.IsExpired())
	assert.Nil(t, c.Get("pending"))
	assert.Contains(t, c.items.values, "pending")
}

func TestConsistency_ObservationAPIsDoNotTouchOrSweep(t *testing.T) {
	c := New[string, string](
		WithTTL[string, string](time.Hour),
		WithMaxCost[string, string](100, func(item CostItem[string, string]) uint64 {
			return 1
		}),
	)
	touched := c.Set("touched", "v", time.Hour)
	permanent := c.Set("permanent", "v", NoTTL)
	pending := addInternalExpiredItem(c, "pending")
	oldTouchedExpiresAt := touched.expiresAt

	items := c.Items()
	keys := c.Keys()
	length := c.Len()
	cost := c.Cost()
	metrics := c.Metrics()

	var ranged []string
	c.Range(func(item *Item[string, string]) bool {
		ranged = append(ranged, item.Key())
		return true
	})

	assert.Len(t, items, 2)
	assert.Contains(t, items, "touched")
	assert.Contains(t, items, "permanent")
	assert.NotContains(t, items, "pending")
	assert.Same(t, touched, items["touched"])
	assert.Same(t, permanent, items["permanent"])
	assert.ElementsMatch(t, []string{"touched", "permanent"}, keys)
	assert.Equal(t, 2, length)
	assert.EqualValues(t, 3, cost)
	assert.Equal(t, Metrics{Insertions: 2}, metrics)
	assert.ElementsMatch(t, []string{"permanent", "touched"}, ranged)
	assert.Equal(t, oldTouchedExpiresAt, touched.expiresAt)

	assert.Contains(t, c.items.values, "pending")
	assert.Same(t, pending, c.items.values["pending"].Value)
	assert.Equal(t, 0, pending.queueIndex)
	assert.Equal(t, 3, c.items.expQueue.Len())
	assert.Equal(t, c.items.lru.Len(), c.items.expQueue.Len())
	assert.Equal(t, Metrics{Insertions: 2}, c.Metrics())
}

func TestConsistency_CapacityEvictionIsAtomicUnderWriteLock(t *testing.T) {
	c := New[string, string](
		WithCapacity[string, string](1),
		WithTTL[string, string](time.Hour),
	)
	victim := c.Set("victim", "old", NoTTL)

	checked := make(chan struct{})
	c.events.eviction.fns[1] = func(reason EvictionReason, item *Item[string, string]) {
		assert.Equal(t, EvictionReasonCapacityReached, reason)
		assert.Same(t, victim, item)
		assert.NotContains(t, c.items.values, "victim")
		assert.NotContains(t, c.items.values, "new")
		assert.Equal(t, 0, c.items.lru.Len())
		assert.Equal(t, 0, c.items.expQueue.Len())
		assert.Equal(t, -1, item.queueIndex)
		close(checked)
	}

	incoming := c.Set("new", "v", time.Hour)
	waitForChannel(t, checked, "eviction callback did not reach post-removal barrier")

	c.items.mu.RLock()
	assert.NotContains(t, c.items.values, "victim")
	assert.Same(t, incoming, c.items.values["new"].Value)
	assert.Equal(t, 1, c.items.lru.Len())
	assert.Equal(t, 1, c.items.expQueue.Len())
	assert.Equal(t, -1, victim.queueIndex)
	c.items.mu.RUnlock()
}

func TestConsistency_RangeCanSpanMultipleSerializedStates(t *testing.T) {
	c := New[string, string](
		WithCapacity[string, string](4),
		WithTTL[string, string](time.Hour),
	)
	c.Set("evicted", "old", time.Hour)
	c.Set("a", "old", time.Hour)
	renewed := c.Set("renewed", "old", time.Hour)
	c.Set("replaced", "old", time.Hour)
	oldRenewedExpiresAt := renewed.ExpiresAt()

	atBarrier := make(chan struct{})
	proceed := make(chan struct{})
	observed := make(chan string, 8)
	done := make(chan struct{})

	go func() {
		c.RangeBackwards(func(item *Item[string, string]) bool {
			switch item.Key() {
			case "evicted":
				assert.Equal(t, "old", item.Value())
				observed <- "evicted-before"
			case "a":
				close(atBarrier)
				<-proceed
				observed <- "a"
			case "renewed":
				assert.True(t, item.ExpiresAt().After(oldRenewedExpiresAt))
				observed <- "renewed-after"
			case "replaced":
				assert.Equal(t, "new", item.Value())
				observed <- "replaced-after"
			case "incoming":
				observed <- "incoming-after"
			}
			return true
		})
		close(done)
	}()

	waitForChannel(t, atBarrier, "Range did not reach synchronization barrier")
	c.Get("renewed")
	c.Set("replaced", "new", time.Hour)
	c.Set("incoming", "v", 2*time.Hour)
	close(proceed)
	waitForChannel(t, done, "Range did not finish after mutations")

	close(observed)
	assert.Equal(t,
		[]string{"evicted-before", "a", "renewed-after", "replaced-after", "incoming-after"},
		collectStrings(observed),
	)
	assert.NotContains(t, c.Items(), "evicted")
	assert.Contains(t, c.Items(), "renewed")
	assert.Contains(t, c.Items(), "replaced")
	assert.Contains(t, c.Items(), "incoming")
}

func TestConsistency_ItemsCallExcludesExpiredPendingWhilePhysicalStateHasIt(t *testing.T) {
	c := New[string, string](
		WithCapacity[string, string](4),
		WithMaxCost[string, string](10, func(item CostItem[string, string]) uint64 {
			return 1
		}),
	)
	renewed := c.Set("renewed", "old", time.Hour)
	replaced := c.Set("replaced", "old", time.Hour)
	pending := addInternalExpiredItem(c, "pending")

	oldRenewedExpiresAt := renewed.ExpiresAt()
	c.Get("renewed")
	c.Set("replaced", "new", 2*time.Hour)

	items := c.Items()
	assert.Contains(t, items, "renewed")
	assert.Contains(t, items, "replaced")
	assert.NotContains(t, items, "pending")
	assert.True(t, items["renewed"].ExpiresAt().After(oldRenewedExpiresAt))
	assert.Equal(t, "new", items["replaced"].Value())
	assert.Equal(t, 2, len(items))
	assert.Equal(t, 2, c.Len())
	assert.EqualValues(t, 3, c.Cost())

	c.items.mu.RLock()
	assert.Same(t, renewed, c.items.values["renewed"].Value)
	assert.Same(t, replaced, c.items.values["replaced"].Value)
	assert.Same(t, pending, c.items.values["pending"].Value)
	assert.Equal(t, 3, c.items.lru.Len())
	assert.Equal(t, 3, c.items.expQueue.Len())
	c.items.mu.RUnlock()
}

func TestConsistency_ReadLockBarrierExcludesWriter(t *testing.T) {
	c := New[string, string](WithCapacity[string, string](1))
	victim := c.Set("victim", "v", NoTTL)

	c.items.mu.RLock()
	done := make(chan struct{})
	go func() {
		c.Set("incoming", "v", NoTTL)
		close(done)
	}()

	assert.Contains(t, c.items.values, "victim")
	assert.NotContains(t, c.items.values, "incoming")
	assert.Equal(t, 1, c.items.lru.Len())
	assert.Equal(t, 1, c.items.expQueue.Len())
	assert.Equal(t, 0, victim.queueIndex)
	c.items.mu.RUnlock()

	waitForChannel(t, done, "writer did not complete after read-lock barrier was released")
	assert.NotContains(t, c.Items(), "victim")
	assert.Contains(t, c.Items(), "incoming")
}

func collectStrings(ch <-chan string) []string {
	var result []string
	for value := range ch {
		result = append(result, value)
	}
	return result
}

func TestConsistency_InsertionCallbackCanReadLaterState(t *testing.T) {
	c := New[string, string]()
	releaseCallback := make(chan struct{})
	seen := make(chan string, 1)
	unsubscribe := c.OnInsertion(func(_ context.Context, item *Item[string, string]) {
		<-releaseCallback
		seen <- item.Value()
	})

	c.Set("key", "inserted", NoTTL)
	c.Set("key", "updated", NoTTL)
	close(releaseCallback)

	select {
	case value := <-seen:
		assert.Equal(t, "updated", value)
	case <-time.After(time.Second):
		t.Fatal("insertion callback did not observe item")
	}

	unsubscribe()
}

func TestConsistency_EvictionCallbackRunsOutsideItemsLock(t *testing.T) {
	c := New[string, string]()
	victim := c.Set("victim", "v", NoTTL)
	c.Set("survivor", "v", NoTTL)
	seen := make(chan struct{})
	unsubscribe := c.OnEviction(func(_ context.Context, reason EvictionReason, item *Item[string, string]) {
		assert.Equal(t, EvictionReasonDeleted, reason)
		assert.Same(t, victim, item)
		assert.Equal(t, "victim", item.Key())
		assert.Nil(t, c.Get("victim"))
		assert.True(t, c.Has("survivor"))
		close(seen)
	})

	c.Delete("victim")
	waitForChannel(t, seen, "eviction callback did not finish")
	unsubscribe()
}
