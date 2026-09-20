package ttlcache

import (
	"container/list"
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type syntheticItemSpec struct {
	key       string
	value     string
	ttl       time.Duration
	expiresAt time.Time
}

func newAnalysisCache(maxCost uint64, ttl time.Duration) *Cache[string, string] {
	c := New[string, string](
		WithTTL[string, string](ttl),
		WithVersion[string, string](true),
	)
	if maxCost != 0 {
		c.options.maxCost = maxCost
		c.options.itemOpts = append(c.options.itemOpts, WithItemCostFunc[string, string](
			func(item CostItem[string, string]) uint64 { return uint64(len(item.Value)) },
		))
	}
	return c
}

func addSyntheticItems(t *testing.T, c *Cache[string, string], specs ...syntheticItemSpec) {
	t.Helper()
	for _, spec := range specs {
		item := NewItemWithOpts(spec.key, spec.value, spec.ttl, c.options.itemOpts...)
		if !spec.expiresAt.IsZero() {
			item.expiresAt = spec.expiresAt
		}
		elem := c.items.lru.PushFront(item)
		c.items.values[spec.key] = elem
		c.items.expQueue.push(elem)
		if c.options.maxCost != 0 {
			c.cost += item.cost
		}
	}
}

func lruKeys(c *Cache[string, string]) []string {
	keys := make([]string, 0, c.items.lru.Len())
	for elem := c.items.lru.Front(); elem != nil; elem = elem.Next() {
		keys = append(keys, elem.Value.(*Item[string, string]).key)
	}
	return keys
}

func heapKeys(c *Cache[string, string]) []string {
	keys := make([]string, 0, c.items.expQueue.Len())
	for _, elem := range c.items.expQueue {
		keys = append(keys, elem.Value.(*Item[string, string]).key)
	}
	return keys
}

func mapKeys(m map[string]*list.Element) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	return keys
}

func itemMapKeys(m map[string]*Item[string, string]) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	return keys
}

func assertStructures(t *testing.T, c *Cache[string, string], lru, heap, values []string) {
	t.Helper()
	require.Equal(t, lru, lruKeys(c))
	require.Equal(t, heap, heapKeys(c))
	require.ElementsMatch(t, values, mapKeys(c.items.values))
	require.Len(t, c.items.expQueue, len(heap))
	for index, elem := range c.items.expQueue {
		require.Equal(t, index, elem.Value.(*Item[string, string]).queueIndex)
	}
}

func drainTimer(t *testing.T, c *Cache[string, string], shouldHaveValue bool) {
	t.Helper()
	select {
	case <-c.items.timerCh:
		require.True(t, shouldHaveValue)
	default:
		require.False(t, shouldHaveValue)
	}
}

func at(offset time.Duration) time.Time {
	return time.Now().Add(offset)
}

func TestAnalysisSetNewAndCapacityEviction(t *testing.T) {
	c := newAnalysisCache(0, NoTTL)
	c.options.capacity = 2

	addSyntheticItems(t, c,
		syntheticItemSpec{key: "a", value: "a", ttl: time.Hour, expiresAt: at(time.Hour)},
		syntheticItemSpec{key: "b", value: "b", ttl: NoTTL},
	)
	assertStructures(t, c, []string{"b", "a"}, []string{"a", "b"}, []string{"a", "b"})

	c.Set("c", "c", NoTTL)

	assertStructures(t, c, []string{"c", "b"}, []string{"b", "c"}, []string{"b", "c"})
	require.False(t, c.Has("a"))
	require.Equal(t, uint64(1), c.Metrics().Evictions)
	drainTimer(t, c, false)
}

func TestAnalysisSetReplacesExistingItem(t *testing.T) {
	c := newAnalysisCache(0, time.Hour)
	addSyntheticItems(t, c,
		syntheticItemSpec{key: "a", value: "a", ttl: time.Hour, expiresAt: at(time.Hour)},
		syntheticItemSpec{key: "b", value: "b", ttl: 2 * time.Hour, expiresAt: at(2 * time.Hour)},
	)

	item := c.items.values["a"].Value.(*Item[string, string])
	version := item.version
	replaced := c.Set("a", "new", NoTTL)

	require.Same(t, item, replaced)
	require.Equal(t, int64(version+1), replaced.version)
	require.Equal(t, "new", replaced.Value())
	assertStructures(t, c, []string{"a", "b"}, []string{"b", "a"}, []string{"a", "b"})
	drainTimer(t, c, false)
}

func TestAnalysisGetHitTouches(t *testing.T) {
	c := newAnalysisCache(0, NoTTL)
	addSyntheticItems(t, c,
		syntheticItemSpec{key: "a", value: "a", ttl: NoTTL},
		syntheticItemSpec{key: "b", value: "b", ttl: 2 * time.Hour, expiresAt: at(2 * time.Hour)},
	)

	oldExpiry := c.items.values["b"].Value.(*Item[string, string]).expiresAt
	item := c.Get("b")

	require.NotNil(t, item)
	require.True(t, item.ExpiresAt().After(oldExpiry))
	assertStructures(t, c, []string{"b", "a"}, []string{"b", "a"}, []string{"a", "b"})
	drainTimer(t, c, false)
}

func TestAnalysisGetWithoutTouchMovesLRUOnly(t *testing.T) {
	c := newAnalysisCache(0, NoTTL)
	expectedExpiry := at(2 * time.Hour)
	addSyntheticItems(t, c,
		syntheticItemSpec{key: "a", value: "a", ttl: time.Hour, expiresAt: at(time.Hour)},
		syntheticItemSpec{key: "b", value: "b", ttl: 2 * time.Hour, expiresAt: expectedExpiry},
	)

	item := c.Get("b", WithDisableTouchOnHit[string, string]())

	require.NotNil(t, item)
	require.Equal(t, expectedExpiry, item.ExpiresAt())
	assertStructures(t, c, []string{"b", "a"}, []string{"a", "b"}, []string{"a", "b"})
	drainTimer(t, c, false)
}

func TestAnalysisUpdateTTLShortensAndNotifiesRoot(t *testing.T) {
	c := newAnalysisCache(0, NoTTL)
	addSyntheticItems(t, c,
		syntheticItemSpec{key: "a", value: "a", ttl: time.Hour, expiresAt: at(time.Hour)},
		syntheticItemSpec{key: "b", value: "b", ttl: 2 * time.Hour, expiresAt: at(2 * time.Hour)},
	)

	c.Set("b", "b", time.Minute)

	assertStructures(t, c, []string{"b", "a"}, []string{"b", "a"}, []string{"a", "b"})
	drainTimer(t, c, true)
}

func TestAnalysisDeleteExpiredRemovesRootsAndCost(t *testing.T) {
	c := newAnalysisCache(10, NoTTL)
	addSyntheticItems(t, c,
		syntheticItemSpec{key: "old1", value: "x", ttl: time.Hour, expiresAt: at(-2 * time.Hour)},
		syntheticItemSpec{key: "old2", value: "yy", ttl: time.Hour, expiresAt: at(-time.Hour)},
		syntheticItemSpec{key: "keep", value: "zzz", ttl: time.Hour, expiresAt: at(time.Hour)},
	)
	require.Equal(t, uint64(6), c.Cost())

	c.DeleteExpired()

	assertStructures(t, c, []string{"keep"}, []string{"keep"}, []string{"keep"})
	require.Equal(t, uint64(3), c.Cost())
	require.Equal(t, uint64(2), c.Metrics().Evictions)
}

func TestAnalysisCostEvictionAfterUpdate(t *testing.T) {
	c := newAnalysisCache(3, NoTTL)
	addSyntheticItems(t, c,
		syntheticItemSpec{key: "a", value: "a", ttl: NoTTL},
		syntheticItemSpec{key: "b", value: "b", ttl: NoTTL},
	)

	c.Set("a", "zzz", time.Hour)

	assertStructures(t, c, []string{"a"}, []string{"a"}, []string{"a"})
	require.Equal(t, uint64(3), c.Cost())
	require.Equal(t, uint64(1), c.Metrics().Evictions)
	drainTimer(t, c, true)
}

func TestAnalysisTTLZeroAndNegativeValues(t *testing.T) {
	c := newAnalysisCache(0, time.Hour)

	c.Set("zero-default", "v", DefaultTTL)
	c.Set("negative-one", "v", NoTTL)
	c.Set("negative-other", "v", -3*time.Second)

	assertStructures(t, c,
		[]string{"negative-other", "negative-one", "zero-default"},
		[]string{"zero-default", "negative-one", "negative-other"},
		[]string{"zero-default", "negative-one", "negative-other"},
	)
	for key := range c.items.values {
		item := c.items.values[key].Value.(*Item[string, string])
		require.False(t, item.isExpiredUnsafe())
	}
	drainTimer(t, c, true)
}

func TestAnalysisExpiredRemainsUntilSweepAndCanBeReplaced(t *testing.T) {
	c := newAnalysisCache(0, NoTTL)
	addSyntheticItems(t, c, syntheticItemSpec{
		key: "a", value: "old", ttl: time.Hour, expiresAt: at(-time.Nanosecond),
	})

	require.Nil(t, c.Get("a"))
	require.Contains(t, c.items.values, "a")
	require.Empty(t, c.Items())
	require.Equal(t, 0, c.Len())
	require.Equal(t, []string{"a"}, heapKeys(c))

	item := c.Set("a", "new", NoTTL)

	require.Equal(t, "new", item.Value())
	require.False(t, item.IsExpired())
	assertStructures(t, c, []string{"a"}, []string{"a"}, []string{"a"})
	drainTimer(t, c, false)
}

func TestAnalysisPreviousTTLKeepsExpiredTimestamp(t *testing.T) {
	c := newAnalysisCache(0, time.Hour)
	addSyntheticItems(t, c, syntheticItemSpec{
		key: "a", value: "old", ttl: time.Minute, expiresAt: at(-time.Nanosecond),
	})

	item := c.Set("a", "new", PreviousOrDefaultTTL)

	require.True(t, item.IsExpired())
	require.Nil(t, c.Get("a"))
	require.Contains(t, c.items.values, "a")
	assertStructures(t, c, []string{"a"}, []string{"a"}, []string{"a"})
	drainTimer(t, c, false)
}

func TestAnalysisExpirationNotifications(t *testing.T) {
	t.Run("later root does not notify", func(t *testing.T) {
		c := newAnalysisCache(0, NoTTL)
		addSyntheticItems(t, c,
			syntheticItemSpec{key: "root", value: "v", ttl: time.Hour, expiresAt: at(time.Hour)},
			syntheticItemSpec{key: "other", value: "v", ttl: 2 * time.Hour, expiresAt: at(2 * time.Hour)},
		)

		c.Set("root", "v", 3*time.Hour)

		assertStructures(t, c, []string{"root", "other"}, []string{"other", "root"}, []string{"root", "other"})
		drainTimer(t, c, false)
	})

	t.Run("other item becomes earlier root and notifies", func(t *testing.T) {
		c := newAnalysisCache(0, NoTTL)
		addSyntheticItems(t, c,
			syntheticItemSpec{key: "root", value: "v", ttl: time.Hour, expiresAt: at(time.Hour)},
			syntheticItemSpec{key: "other", value: "v", ttl: 2 * time.Hour, expiresAt: at(2 * time.Hour)},
		)

		c.Set("other", "v", time.Minute)

		assertStructures(t, c, []string{"other", "root"}, []string{"other", "root"}, []string{"root", "other"})
		drainTimer(t, c, true)
	})

	t.Run("root becomes no ttl and does not notify", func(t *testing.T) {
		c := newAnalysisCache(0, NoTTL)
		addSyntheticItems(t, c,
			syntheticItemSpec{key: "root", value: "v", ttl: time.Hour, expiresAt: at(time.Hour)},
			syntheticItemSpec{key: "other", value: "v", ttl: 2 * time.Hour, expiresAt: at(2 * time.Hour)},
		)

		c.Set("root", "v", NoTTL)

		assertStructures(t, c, []string{"root", "other"}, []string{"other", "root"}, []string{"root", "other"})
		drainTimer(t, c, false)
	})

	t.Run("earlier pending duration is retained", func(t *testing.T) {
		c := newAnalysisCache(0, NoTTL)
		c.items.timerCh <- time.Nanosecond
		addSyntheticItems(t, c, syntheticItemSpec{
			key: "root", value: "v", ttl: time.Hour, expiresAt: at(time.Hour),
		})

		c.Set("root", "v", time.Minute)

		select {
		case d := <-c.items.timerCh:
			require.Equal(t, time.Nanosecond, d)
		default:
			t.Fatal("expected timer notification")
		}
	})

	t.Run("touch on non root fixes index without notification", func(t *testing.T) {
		c := newAnalysisCache(0, NoTTL)
		addSyntheticItems(t, c,
			syntheticItemSpec{key: "root", value: "v", ttl: time.Hour, expiresAt: at(time.Hour)},
			syntheticItemSpec{key: "other", value: "v", ttl: 2 * time.Hour, expiresAt: at(2 * time.Hour)},
		)

		item := c.Get("other")

		require.NotNil(t, item)
		assertStructures(t, c, []string{"other", "root"}, []string{"root", "other"}, []string{"root", "other"})
		drainTimer(t, c, false)
	})
}

func TestAnalysisStartSweepsExpiredAndStopPreservesState(t *testing.T) {
	c := newAnalysisCache(0, NoTTL)
	c.stopped = true
	addSyntheticItems(t, c,
		syntheticItemSpec{key: "expired", value: "v", ttl: time.Hour, expiresAt: time.Now().Add(-time.Hour)},
		syntheticItemSpec{key: "keep", value: "v", ttl: NoTTL},
	)
	evicted := make(chan *Item[string, string], 1)
	swept := make(chan bool, 1)
	c.events.eviction.fns[1] = func(_ EvictionReason, item *Item[string, string]) {
		evicted <- item
		if item.key == "expired" {
			_, present := c.items.values["expired"]
			swept <- !present
		}
	}

	var started sync.WaitGroup
	started.Add(1)
	ready := make(chan struct{})
	go func() {
		started.Done()
		close(ready)
		c.Start()
	}()
	started.Wait()
	<-ready

	require.Equal(t, "expired", (<-evicted).key)
	require.True(t, <-swept)
	require.True(t, c.Has("keep"))

	c.Stop()
	require.True(t, c.stopped)
	require.Contains(t, c.items.values, "keep")

	c.Stop()
	require.True(t, c.stopped)
}

func TestAnalysisPublicEvictionCallbackRunsWithoutItemsLock(t *testing.T) {
	c := newAnalysisCache(0, NoTTL)
	addSyntheticItems(t, c,
		syntheticItemSpec{key: "a", value: "v", ttl: NoTTL},
	)
	entered := make(chan bool, 1)
	release := make(chan struct{})
	unsubscribe := c.OnEviction(func(ctx context.Context, reason EvictionReason, item *Item[string, string]) {
		if item.Key() == "a" {
			locked := c.items.mu.TryRLock()
			if locked {
				c.items.mu.RUnlock()
			}
			entered <- locked
			select {
			case <-release:
			case <-ctx.Done():
			}
		}
	})

	c.Delete("a")
	require.True(t, <-entered)

	close(release)
	unsubscribe()
	require.NotContains(t, c.items.values, "a")
}

func TestAnalysisObservationsDoNotTouchOrSweep(t *testing.T) {
	c := newAnalysisCache(10, NoTTL)
	addSyntheticItems(t, c,
		syntheticItemSpec{key: "expired", value: "x", ttl: time.Hour, expiresAt: at(-time.Nanosecond)},
		syntheticItemSpec{key: "live", value: "yyyyy", ttl: time.Hour, expiresAt: at(time.Hour)},
	)
	expiredExpiry := c.items.values["expired"].Value.(*Item[string, string]).expiresAt
	liveExpiry := c.items.values["live"].Value.(*Item[string, string]).expiresAt

	require.Equal(t, 1, c.Len())
	require.ElementsMatch(t, []string{"live"}, c.Keys())
	require.ElementsMatch(t, []string{"live"}, itemMapKeys(c.Items()))
	require.True(t, c.Has("live"))
	require.False(t, c.Has("expired"))

	var ranged []string
	c.Range(func(item *Item[string, string]) bool {
		ranged = append(ranged, item.Key())
		return true
	})
	c.RangeBackwards(func(item *Item[string, string]) bool {
		ranged = append(ranged, item.Key())
		return true
	})

	require.Equal(t, uint64(6), c.Cost())
	require.Equal(t, []string{"live", "live"}, ranged)
	require.Equal(t, expiredExpiry, c.items.values["expired"].Value.(*Item[string, string]).expiresAt)
	require.Equal(t, liveExpiry, c.items.values["live"].Value.(*Item[string, string]).expiresAt)
	require.Equal(t, []string{"live", "expired"}, lruKeys(c))
	require.Contains(t, c.items.values, "expired")
	require.Zero(t, c.Metrics().Hits)
	require.Zero(t, c.Metrics().Misses)
	require.Zero(t, c.Metrics().Evictions)
}

func TestAnalysisOneItemsCallCanSeeRenewedReplacedAndEvictionCandidate(t *testing.T) {
	c := newAnalysisCache(0, time.Hour)
	c.options.capacity = 3
	addSyntheticItems(t, c,
		syntheticItemSpec{key: "renewed", value: "v", ttl: time.Hour, expiresAt: at(time.Minute)},
		syntheticItemSpec{key: "replaced", value: "old", ttl: time.Hour, expiresAt: at(2 * time.Hour)},
		syntheticItemSpec{key: "candidate", value: "v", ttl: time.Hour, expiresAt: at(3 * time.Hour)},
	)

	renewedBefore := c.items.values["renewed"].Value.(*Item[string, string]).expiresAt
	renewed := c.Get("renewed")
	replaced := c.Set("replaced", "new", PreviousOrDefaultTTL)

	items := c.Items()
	require.Same(t, renewed, items["renewed"])
	require.Same(t, replaced, items["replaced"])
	require.Same(t, c.items.values["candidate"].Value, items["candidate"])
	require.True(t, renewed.ExpiresAt().After(renewedBefore))
	require.Equal(t, "new", items["replaced"].Value())
	require.Equal(t, "candidate", c.items.lru.Back().Value.(*Item[string, string]).key)
}

func TestAnalysisItemsIsShallowCopyNotItemSnapshot(t *testing.T) {
	c := newAnalysisCache(0, time.Hour)
	c.Set("key", "old", NoTTL)
	first := c.Items()

	c.Set("key", "new", NoTTL)

	require.Equal(t, "new", first["key"].Value())
}

func TestAnalysisItemsCannotObserveHalfFinishedCapacityEviction(t *testing.T) {
	c := newAnalysisCache(0, NoTTL)
	c.options.capacity = 1
	addSyntheticItems(t, c, syntheticItemSpec{key: "old", value: "old", ttl: NoTTL})

	type barrier struct {
		lockHeld bool
		values   []string
	}
	reached := make(chan barrier, 1)
	release := make(chan struct{})
	c.events.eviction.fns[1] = func(_ EvictionReason, _ *Item[string, string]) {
		lockHeld := c.items.mu.TryRLock() == false
		reached <- barrier{lockHeld: lockHeld, values: mapKeys(c.items.values)}
		<-release
	}

	done := make(chan struct{})
	go func() {
		c.Set("new", "new", NoTTL)
		close(done)
	}()

	b := <-reached
	require.True(t, b.lockHeld)
	require.Empty(t, b.values)
	require.False(t, c.items.mu.TryRLock())

	close(release)
	<-done
	require.ElementsMatch(t, []string{"new"}, mapKeys(c.items.values))
	require.ElementsMatch(t, []string{"new"}, itemMapKeys(c.Items()))
}

func TestAnalysisRangeCallbackRunsOutsideItemsLock(t *testing.T) {
	c := newAnalysisCache(0, NoTTL)
	addSyntheticItems(t, c,
		syntheticItemSpec{key: "a", value: "v", ttl: NoTTL},
		syntheticItemSpec{key: "b", value: "v", ttl: NoTTL},
	)

	var seen []string
	c.Range(func(item *Item[string, string]) bool {
		seen = append(seen, item.Key())
		if item.Key() == "a" {
			c.Delete("b")
		}
		return true
	})

	require.Equal(t, []string{"b", "a"}, seen)
	require.ElementsMatch(t, []string{"a"}, mapKeys(c.items.values))
}

func TestAnalysisMetricsAreNotAtomicWithItemsOrCost(t *testing.T) {
	c := newAnalysisCache(10, NoTTL)
	addSyntheticItems(t, c,
		syntheticItemSpec{key: "a", value: "xxx", ttl: NoTTL},
		syntheticItemSpec{key: "b", value: "yyy", ttl: NoTTL},
	)

	type snapshot struct {
		keys []string
		cost uint64
	}
	reached := make(chan snapshot, 1)
	release := make(chan struct{})
	c.events.eviction.fns[1] = func(_ EvictionReason, _ *Item[string, string]) {
		reached <- snapshot{keys: mapKeys(c.items.values), cost: c.cost}
		<-release
	}

	done := make(chan struct{})
	go func() {
		c.Delete("a")
		close(done)
	}()

	s := <-reached
	require.ElementsMatch(t, []string{"b"}, s.keys)
	require.Equal(t, uint64(3), s.cost)
	require.Equal(t, uint64(1), c.Metrics().Evictions)

	close(release)
	<-done
}
