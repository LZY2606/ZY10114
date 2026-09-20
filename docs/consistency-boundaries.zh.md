# ttlcache 一致性边界分析

本文分析当前仓库中 `Cache`、`Item`、LRU 双向链表、`expirationQueue`、`Start` 清理循环和事件回调的实际协作方式。结论以实现为准；配套测试见 `consistency_analysis_test.go`。

关键源码位置：`cache.go:27` 定义 `Cache` 状态，`cache.go:95` 是 `updateExpirations`，`cache.go:140` 和 `cache.go:227` 是 `set`/`get`，`cache.go:304` 是 `evict`，`cache.go:492` 是 `DeleteExpired`，`cache.go:680` 是 `Start`；`item.go:30` 定义 `Item`，`expiration_queue.go:10` 定义 heap。

## 1. 三套结构各回答什么问题

- `items.values map[K]*list.Element`：键是否仍在缓存中，以及映射到哪个链表元素。
- `items.lru *list.List`：最近访问顺序。`Front` 最新，`Back` 是容量或 cost 驱逐候选。
- `items.expQueue`：按 `Item.expiresAt` 排序的最小堆；`expQueue[0]` 是下一个有限 TTL 的到期点。

所有条目都同时位于 map、LRU 链表和 heap 中，包括 `ttl <= 0` 的条目的 `expiresAt` 零值。堆的 `Less` 将零时间排在最后，所以 `NoTTL` 条目也保留在堆中但不会成为到期根节点。

`Item.queueIndex` 是堆内位置。`heap.Push`、`heap.Fix`、`heap.Remove` 通过 `Swap` 或 `Pop` 同步该索引。结构迁移测试同时检查 map 键集合、链表顺序、堆数组顺序和每个 `queueIndex`。

## 2. 锁的职责

- `items.mu`：串行修改或一致读取 map、LRU、heap、`cache.cost` 和 `Item` 的内部字段。
- `Item.mu`：保护从缓存外部读取或更新单个条目的字段；缓存内部持有 `items.mu` 时直接读写这些字段。
- `metricsMu`：只保护聚合计数器。
- `events.<kind>.mu`：只保护订阅表，不保护缓存数据。
- `stopMu`：保护 `stopped` 和 `Start`/`Stop` 生命周期。

`Set`、`Delete`、`DeleteExpired` 持写锁；`Items`、`Keys`、`Len`、`Has`、`Cost` 持读锁。`Get` 也持写锁，因为命中时会执行 `MoveToFront`，默认还会 `touch` 和 `heap.Fix`。

## 3. 路径状态图

以下用 `M` 表示 map，`L` 表示从前到后的 LRU，`H` 表示堆数组；零到期时间的条目排在最后。

### Set：新元素与容量驱逐

容量满时，`set` 先调用 `evict(CapacityReached, lru.Back())`，旧元素从三套结构删除；随后新 `Item` 才 `PushFront`、写入 map、加入 heap。

初始容量 2，已有 `a(1h)`、`b(NoTTL)`，且 LRU 中 `a` 在尾部：

```text
M: a, b
L: b -> a
H: a(1h) -> b(zero)

Set(c, NoTTL)

M: b, c
L: c -> b
H: b(zero) -> c(zero)
```

驱逐回调包装函数运行时，旧元素已经从 map、链表和 heap 移除，新元素尚未插入。容量屏障测试证明此时外部观察无法获得 `items.mu` 读锁，因此不能观察到这个中间态。

### Set：替换同键元素

`set` 命中现有元素时不分配新的 `Item`，而是复用原 `list.Element` 和 `Item` 指针，然后调用 `item.update`。因此：

- map 键和 LRU 节点身份不变；
- LRU 被 `MoveToFront`；
- value、ttl、expiresAt、version 和 cost 在同一个 `Item` 上更新；
- heap 使用同一个 `queueIndex` 执行 `heap.Fix`；
- 订阅者和之前的 `Get` 调用者持有同一个 `*Item`。

例如 `a(1h)` 被改成 `NoTTL`：

```text
M: a, b
L: a -> b
H: b(2h) -> a(zero)
```

`Items` 返回的是 map 的浅拷贝，不复制 `Item`。后续替换会改变先前返回指针的 `Value()`；测试 `TestAnalysisItemsIsShallowCopyNotItemSnapshot` 验证该最小保证。

### Get：命中并续期

`get` 先从 map 读取；若条目已到期且 `includeExpired=false`，直接返回 nil，不移动 LRU、不 touch、不清理。未到期命中时总是先 `MoveToFront`，随后只有 `touch=true` 且 `item.ttl > 0` 才更新到期时间并 `heap.Fix`。

`b(2h)` 是唯一有限 TTL 条目时，命中续期把它推到更晚时间：

```text
L: b -> a
H: b(now+2h 之后) -> a(zero)
```

根节点如果变得更晚，不唤醒后台循环，当前定时器只会提前醒来。

### Get：禁止 touch

`WithDisableTouchOnHit` 只跳过 `item.touch` 和 heap 更新，不跳过 LRU 更新：

```text
expiresAt: 不变
H:         不变，queueIndex 不变
L:         被访问元素移动到 Front
```

因此“禁止 touch”不是“纯读”。

### Update TTL

将较晚的 `b(2h)` 改成 1 分钟：

```text
before H: a(1h) -> b(2h)
after  H: b(1m) -> a(1h)
```

`item.update` 先把新的 `expiresAt` 写入，`updateExpirations(false,...)` 再用该元素原有的 `queueIndex` 调 `heap.Fix`。

### DeleteExpired

`DeleteExpired` 从 heap 根节点开始删除。最小堆保证已到期的有限 TTL 元素位于堆的一个连续前缀；根节点变成未到期或堆为空后停止。每次删除都同步移除 map 节点、LRU 节点和 heap 节点；启用 cost 时同时扣减 cost。

```text
before M: old1, old2, keep
       H: old1(-2h), old2(-1h), keep(1h)

after  M: keep
       L: keep
       H: keep(1h)
```

已经逻辑到期但尚未清扫的元素仍在三套结构中；`Get`、`Has`、`Items`、`Keys`、`Len` 会过滤它，但 `Cost` 在启用 cost 限制时仍计入它。

### Cost 驱逐

cost 驱逐发生在新元素插入和 cost 累加之后，或现有元素更新成本之后；循环驱逐 `lru.Back()`，直到 `cache.cost <= maxCost`。

将已有元素 `a` 从成本 1 更新为成本 3，且 `maxCost=3` 时，尾部的 `b` 被驱逐：

```text
before M: a, b ; cost=2
after  M: a    ; cost=3
```

容量驱逐发生在插入前；cost 驱逐发生在插入或更新后。因此二者的回调可见中间态不同，但都被 `items.mu` 保护。

## 4. `oldExpiresAt`、`heap.Fix` 和后台唤醒

`updateExpirations(fresh, elem, oldExpiresAt)` 的顺序是：

1. 新元素执行 `heap.Push`，已有元素先完成 `Item` 字段修改后执行 `heap.Fix`。
2. 读取调整后的 `expQueue[0].Value.(*Item).expiresAt`。
3. 与调用方在任何元素/heap 变化之前捕获的 `oldExpiresAt` 比较。
4. 只有需要缩短等待时，才向容量为 1 的 `timerCh` 发送 duration。

`oldExpiresAt` 必须是变化前的根时间，而不是被修改元素自己的旧时间。原因是通知条件取决于后台 goroutine 当前等待的“全局最早到期点”是否变早：

- 根节点自己变晚：不通知；旧定时器会提前醒来，`DeleteExpired` 发现根节点未到期后重新计算等待时间。
- 非根节点变成更早的根：通知；否则旧定时器会等到旧根节点时间，导致新根延迟过期。
- 根节点变成零 TTL，且后面有其他有限 TTL：不通知；旧定时器提前醒来后重新以新根等待。
- 新根是零时间或新根不比旧根早：不通知。
- heap 原本没有有限根：新的有限根需要通知。

`heap.Fix` 与通知不是同一个条件。只要已有元素的 `expiresAt` 可能改变，就要 `Fix` 以维护 `queueIndex` 和堆序；通知只在最早 deadline 变早时发生。

`timerCh` 的容量为 1。发送前若已有待处理 duration，代码会非阻塞排空，并保留其中更小的值。这样多个提前到期更新合并后，后台循环不会丢失一个更早的唤醒；较晚的 duration 被丢弃只会导致提前唤醒，不会延迟过期。

### 通知顺序调换的结果

当前顺序是“先改变堆，再按新旧根决定通知”。若改成“先按某种比较发送通知，再修改堆”：

- 通知读取到的根仍可能是旧根，无法基于最终堆状态判断新的等待时间。
- goroutine 可能用旧根对应的 duration 重置定时器，然后 heap 才出现更早根。在新通知到来前，这个更早元素会被延迟清扫。
- 单纯提前等待不会让元素提前从 map 中消失，因为定时器触发后仍以当前时间和 heap 根重新执行 `DeleteExpired`；真正的风险是延迟过期，而不是提前删除。

## 5. TTL 与未清扫状态

- `DefaultTTL == 0`：不是“立即过期”。`set` 先将其解析为 cache 的默认 TTL；默认 TTL 本身为 0 时，`touchUnsafe` 对 `ttl <= 0` 不设置到期时间，因此永不过期。
- `NoTTL == -1`：永不过期，`expiresAt` 为零，仍在 heap 中但排在最后。
- 其他负数：实现同样按 `ttl <= 0` 处理，也永不过期；公开语义只承诺 `NoTTL`，其他负数应视为依赖实现的未定义输入。
- 正 TTL：`isExpiredUnsafe` 用 `expiresAt.Before(time.Now())` 判断，到期时刻本身不算过期。
- 已到期未清扫：map/LRU/heap 仍含该元素；过滤型读访问看不到，物理结构和 cost 仍包含。
- `Set(key,...,PreviousOrDefaultTTL)`：替换 value 和 cost，但保留原 TTL；若元素已经到期，`expiresAt` 不会被重置，替换后仍被读访问过滤。
- `Set(key,...DefaultTTL)`：会按默认 TTL 重新 touch，可以把已到期元素复活。
- `Get` 和 `Touch`：只接受未到期元素，不能复活已到期元素。
- 容量或 cost 驱逐：不区分逻辑是否已到期，可直接把未清扫元素从物理结构移除。

## 6. `Start`/`Stop` 循环

新建的 `Cache.stopped` 为 true；不调用 `Start` 时不会有后台清扫。`Start` 标记运行后建立初始定时器：

- 根节点是正 duration：等待该 duration；
- 根节点已经到期：等待 1 微秒，然后调用 `DeleteExpired`；
- 没有有限根且默认 TTL 为正：等待默认 TTL；
- 否则等待 1 小时。

定时器触发后，`DeleteExpired` 在写锁内清扫；清扫结束后重新计算下一次等待。来自 `timerCh` 的更新会停止并重置定时器。`Stop` 发送 `stopCh` 并等待 `Start` 返回，然后设置 `stopped=true`。`Stop` 不清空 map、LRU、heap 或 cost；停止后已经到期的元素继续保留，直到再次启动、手动 `DeleteExpired`、被驱逐或被替换。

`stopCh` 是复用的无缓冲通道。一个 `Start` 完全 `Stop` 后可以再次启动；重复 `Start` 在已经运行时直接返回。配套测试通过通道等待回调确认实际清扫，再调用 `Stop`，不依赖 goroutine 调度概率。

## 7. 观察 API 的最小保证

| API | 持有的锁 | 是否 touch/LRU | 是否清扫 | 是否包含逻辑到期项 | 观察结果 |
| --- | --- | --- | --- | --- | --- |
| `Items` | `items.mu.RLock` 全程 | 否 | 否 | 否 | 新建 map，但值是共享的 `*Item` 指针 |
| `Keys` | 读锁全程 | 否 | 否 | 否 | 新 key slice |
| `Len` | 读锁全程 | 否 | 否 | 过滤 | heap BFS 计数；极端情况下时间在遍历中前进，结果可能偏小 |
| `Has` | 读锁 | 否 | 否 | 否 | 单次布尔结果 |
| `Cost` | 读锁 | 否 | 否 | 是 | `cache.cost` 当前值；未清扫到期项仍计入 |
| `Range` | 检查在锁内，用户回调在锁外 | 否 | 否 | 检查时过滤 | 迭代期间允许缓存变化 |
| `RangeBackwards` | 同 `Range` | 否 | 否 | 检查时过滤 | 同上，方向相反 |
| `Metrics` | `metricsMu.RLock` | 否 | 否 | 不适用 | 五个计数器的一致拷贝，不与 items/cost 组成同一快照 |

### 单次调用内部

`Items`、`Keys`、`Len` 和 `Cost` 在一次调用持有 `items.mu` 期间不会与 `Set`、`Delete`、`Get` 的写操作交错。它们可以排除“半完成 map/LRU/heap 迁移”。但这不等于返回后仍是快照：

- 返回 map 只复制键到指针的映射，不复制 `Item`；
- 后续 `Set` 复用同一 `*Item` 修改 value、ttl 和 version；
- 返回后条目可能立刻到期或被删除；
- 条目甚至可能在通过到期检查之后、用户通过指针读取字段之前到期。

`Range` 的保证更弱。它在锁内判断当前元素是否到期并保存局部变量，然后释放读锁再调用用户函数。用户函数内可以安全地回调同一个 cache；测试在第一个回调中删除另一个已捕获节点，证明迭代不提供全局快照。

### 跨 API

`Items`/`Keys`/`Len`/`Cost`/`Metrics` 之间没有共享读锁，也没有全局版本号。连续两次调用可能落入不同写操作的前后。尤其是：

- `Len` 排除未清扫到期项，而启用 maxCost 时 `Cost` 包含它；
- `Metrics.Evictions` 先在 `evict` 中增加，随后 map/LRU/heap 才删除；
- 更新路径中 cost 已更新、事件还未派发；
- 插入路径中结构已插入、metrics 和事件稍后更新。

因此，不应从名称推断“线性一致的多维状态快照”。最小可观测保证是：每个 API 对其自身锁保护的数据结构给出一次合法的原子读取；跨 API 或返回后的对象内容不承诺同一逻辑时刻。

## 8. 回调看到的状态

注册函数的包装器保存在独立的 events map 中。`set` 和 `evict` 持有事件类型的 `RLock` 遍历包装器；包装器执行 `wg.Add(1)` 并 `go fn(...)`，随后立即返回。

由此得到两层结论：

1. 包装器派发点位于缓存操作的 `items.mu` 临界区内。单个驱逐元素在调用包装器前已经从三套结构删除；插入/更新元素在调用包装器前已经写入并完成 heap 调整。
2. 用户提供的 `fn` 在新 goroutine 中运行，不持有 `items.mu`，也不保证已经被调度。`Set`/`Delete` 返回不会等待回调完成；只有退订函数会 `cancel`、摘除包装器并 `wg.Wait()` 该订阅已启动的回调。

回调收到的是同一个 `*Item`，不是值快照。公开回调可以在其中调用 cache 方法；测试用 `TryRLock` 证明回调 goroutine 能获得读锁。若直接在内部同步包装器中再调用 `Has` 则会自死锁，这也反证包装器派发点仍在写锁内。

`DeleteAll` 的顺序与单元素驱逐不同：它先在旧 map 中同步派发所有驱逐包装器，然后才替换 map、初始化链表和 heap。因此回调看到的元素已经是“待从旧结构断开”的逻辑驱逐状态，但物理结构尚未整体替换。公开回调仍然异步、无锁。

## 9. 两个并发时序

### 可能发生：一次 `Items` 调用包含三类角色

容量为 3，初始有 `renewed`、`replaced`、`candidate`：

1. goroutine A 调用 `Get("renewed")`，完成 LRU 前移和续期。
2. goroutine A 调用 `Set("replaced",...,PreviousOrDefaultTTL)`，在原 `Item` 上替换 value，保留 TTL，并把它移到 LRU 前端。
3. 此时尚未插入第 4 个键，`candidate` 位于 LRU 尾部，是下一次容量驱逐候选。
4. goroutine B 的 `Items()` 获得读锁，一次 map 遍历可以同时返回续期后的 `renewed`、被替换后的同一个 `replaced` 指针和待驱逐的 `candidate`。

这里“待驱逐”只表示 LRU 尾部候选，不表示已经删除。测试在确定性的顺序中构造该状态，并用 `Items()` 返回的指针身份验证。

### 被锁排除：容量驱逐的中间状态

容量为 1，已有 `old`：

1. goroutine A 执行 `Set("new",...)` 并持有 `items.mu` 写锁。
2. `evict` 删除 `old` 的 map/LRU/heap 节点，计数增加，然后进入驱逐事件包装器屏障。
3. 此时 `new` 尚未插入；内部屏障能看到 map 为空且 `TryRLock` 失败。
4. goroutine B 的 `Items()` 阻塞在 `RLock`，不能观察“map 为空”的中间态。
5. A 释放屏障，插入 `new`，返回并解锁。
6. B 只能获得调用前的 `{old}` 或调用后的 `{new}`，二者由锁互斥决定，不会在一次 `Items()` 中同时看到两者，也不会看到空 map。

`Range` 用户回调除外：它故意在锁外执行，因此可以与其他写操作交错；这是实现明确允许的重入行为，不是快照漏洞。

## 10. 文档与实现差异、兼容影响

- README 说明默认条目不过期，需用 `WithTTL` 并启动清理；这与 `DefaultTTL=0` 最终解析到全局 TTL、全局 TTL 0 又被当作无 TTL 一致。
- README 说明事件 handler 在独立 goroutine 中执行、不阻塞 cache 操作；实现确实不等待用户回调，但包装器的派发点仍在 `items.mu` 临界区内。用户回调本身无锁；这是更精确的锁边界，不改变公开用法。
- `Items` 注释称返回所有 items 的 copy，但实现过滤到期项且只浅拷贝 map。依赖“返回后值不变”或“包含未清扫到期项”的调用会不兼容；依赖当前 key 集合和 `*Item` 身份的调用不受影响。
- `Cost` 注释称返回当前存储 items 的总成本；未清扫到期项仍在内部结构中，因此计入 cost，而 `Len` 不计数。跨这两个 API 做容量不变量没有实现保证。

## 11. 对应测试

- `TestAnalysisSetNewAndCapacityEviction`：新元素、容量驱逐和三套结构状态。
- `TestAnalysisSetReplacesExistingItem`：同指针替换、版本与 heap 位置。
- `TestAnalysisGetHitTouches` / `TestAnalysisGetWithoutTouchMovesLRUOnly`：touch 与只移动 LRU 的区别。
- `TestAnalysisUpdateTTLShortensAndNotifiesRoot`：TTL 缩短、`heap.Fix` 和通知。
- `TestAnalysisDeleteExpiredRemovesRootsAndCost`：从 heap 根连续清扫并扣减 cost。
- `TestAnalysisCostEvictionAfterUpdate`：更新导致的 cost 驱逐。
- `TestAnalysisTTLZeroAndNegativeValues`、`TestAnalysisExpiredRemainsUntilSweepAndCanBeReplaced`、`TestAnalysisPreviousTTLKeepsExpiredTimestamp`：TTL=0、负数、未清扫和复活边界。
- `TestAnalysisExpirationNotifications`：根提前、延后、变零、待处理短 duration 合并和非根 touch。
- `TestAnalysisStartSweepsExpiredAndStopPreservesState`：启动清扫与 Stop 保留状态。
- `TestAnalysisPublicEvictionCallbackRunsWithoutItemsLock`：公开回调不持有 items 锁。
- `TestAnalysisObservationsDoNotTouchOrSweep`：观察 API 不 touch、不清理以及 cost 差异。
- `TestAnalysisOneItemsCallCanSeeRenewedReplacedAndEvictionCandidate`：一次可能观察到的三类条目。
- `TestAnalysisItemsIsShallowCopyNotItemSnapshot`：浅拷贝和共享 `Item`。
- `TestAnalysisItemsCannotObserveHalfFinishedCapacityEviction`：被锁排除的中间态。
- `TestAnalysisRangeCallbackRunsOutsideItemsLock`：`Range` 回调锁外执行。
- `TestAnalysisMetricsAreNotAtomicWithItemsOrCost`：metrics 与 items/cost 不是同一快照。
