# ttlcache 缓存状态一致性分析

本文从实现而不是 API 名称出发，追踪 `Cache.items.values`、`items.lru`、
`items.expQueue`、`Item.queueIndex`、后台 `Start` 循环和事件回调如何共同维护
缓存状态。所有结论均由 `consistency_analysis_test.go` 中的白盒测试验证；测试只
使用 channel、关闭事件和内部锁作为同步屏障，失败保护才使用超时，不把 sleep
或调度时机当作正确性条件。

## 1. 状态模型与锁边界

核心字段在 `cache.go:27`：

- `items.values map[K]*list.Element`：key 到链表元素的索引。
- `items.lru *list.List`：最近访问顺序，链表头最新、链表尾最旧。
- `items.expQueue expirationQueue[K,V]`：按 `Item.expiresAt` 排序的最小堆。
- `items.timerCh chan time.Duration`：容量为 1 的定时器建议通道。
- `cost`：启用 `WithMaxCost` 时维护的物理 cost 总和。

一个条目从创建开始就同时进入 map、链表和 heap，即使它使用 `NoTTL` 或其他非
正 TTL 也是如此；非正 TTL 的 `expiresAt` 为零值，在堆比较中被排到有限到期时间
之后，而不是从 heap 删除。删除时 `heap.Pop` 才把 `queueIndex` 置为 `-1`
（`expiration_queue.go:75`）。这是分析中最重要的基础事实之一。

所有会改变这三份结构的公开操作都持有 `items.mu.Lock()`，包括 `Set`、`Delete`、
`DeleteAll`、`DeleteExpired`、`Touch`（`cache.go:375`、`cache.go:389`、
`cache.go:484`、`cache.go:491`、`cache.go:513`）。`Get` 持有同一把写锁；
`GetOrSet`、`GetAndDelete` 等复合写路径也如此。它们调用内部 `get` 时
`includeExpired=true` 或 `false`，但更新 map/heap 的操作仍被同一个临界区串行化。

`Items`、`Keys`、`Len`、`Cost` 和 `Range` 的遍历期间使用 `items.mu.RLock()`，
因此写操作不能在其持有的观察区间内提交。区别是：

- `Items`、`Keys`、`Len`、`Cost` 从入口到返回一直持有读锁。
- `Range`、`RangeBackwards` 在每个节点判断过期后释放读锁，再在锁外执行用户函数，
  返回后重新加锁取下一个节点（`cache.go:602`、`cache.go:626`）。
- 事件 map 各有独立的 `events.*.mu`；metrics 有独立的 `metricsMu`；单个 `Item`
  的公开读取方法使用 `Item.mu`。这些锁都不能替代 `items.mu` 给出缓存级快照。

## 2. 记法

下面的状态按 `map / LRU 头→尾 / heap 根→叶` 表示。

- `k(ttl,expiry)` 表示一个 `Item` 指针。
- `NoTTL` 表示 `ttl<=0` 且 `expiresAt` 为零值。
- `expired-pending` 表示逻辑时间已经超过 `expiresAt`，但尚未被 `DeleteExpired`、
  删除或驱逐清出物理结构。
- `idx` 是该条目在 heap slice 中的当前下标。

## 3. 七条路径的状态变化

### 3.1 `Set`：新插入

`set` 在 `cache.go:190` 先捕获变更前的 heap 根 `oldExpiresAt`，然后创建
`Item`、`list.PushFront`、写 map、再通过 `updateExpirations(true,...)` 执行
`heap.Push`（`cache.go:195`-`cache.go:199`）。

例：空缓存，先插入 `a` 且 `NoTTL`：

| 阶段 | map | LRU | heap |
| --- | --- | --- | --- |
| 前 | `{}` | `[]` | `[]` |
| 后 | `{a:A}` | `[A]` | `[A(NoTTL)]`，`A.idx=0` |

`NoTTL` 条目仍在 heap 中，所以 `A.queueIndex` 是 `0`，不是 `-1`。此时新根是
零值，`updateExpirations` 不通知后台。

### 3.2 `Get` 命中并续期

公开 `Get` 使用写锁并调用 `get(..., touch=true, includeExpired=false)`
（`cache.go:262`-`cache.go:278`）。命中时顺序是：

1. 若 `item.isExpiredUnsafe()` 为真，直接返回 nil；过期判断发生在移动 LRU 前
   （`cache.go:233`-`cache.go:236`）。
2. `lru.MoveToFront(elem)`。
3. 仅当 `touch==true && item.ttl>0` 时捕获旧根、调用 `item.touch()`，再
   `heap.Fix` 和可能通知后台（`cache.go:240`-`cache.go:247`）。

例：`a=1h`，`b=2h`，插入顺序为 `a`、`b`：

| 阶段 | map | LRU | heap |
| --- | --- | --- | --- |
| `Get(a)` 前 | `{a,b}` | `[B,A]` | `[A@1h, B@2h]` |
| MoveToFront 后 | `{a,b}` | `[A,B]` | `[A@1h, B@2h]` |
| touch 后 | `{a,b}` | `[A,B]` | `[A@now+1h, B@2h]` |

touch 后的截止时间一定晚于原先同一条目的截止时间，但它不一定改变 heap 根。
如果它仍然是根，新根并未严格早于旧根，不发送通知；测试通过先构造一个更早的
固定截止时间验证了“只有根提前才通知”，而不是“每次 touch 都通知”。

### 3.3 `Get` 禁止 touch

`WithDisableTouchOnHit` 只把 `touch` 参数变为 false；`MoveToFront` 仍然执行。
因此“禁止 touch”只禁止续期和 heap 重排，不冻结 LRU 近因顺序。

例：把 `a.expiresAt` 固定到 30 分钟后，然后 `Get(a, WithDisableTouchOnHit())`：

| 阶段 | map | LRU | heap |
| --- | --- | --- | --- |
| 前 | `{a,b}` | `[B,A]` | `[A@30m, B@2h]` |
| 后 | `{a,b}` | `[A,B]` | `[A@30m, B@2h]`，idx 不变 |

### 3.4 `Set` 更新 TTL / 替换同 key

同 key 更新复用原 `Item` 指针，不新建链表节点。内部 `set` 先调用
`get(key,false,true)`，因此即便条目已经逻辑过期但尚未清扫，也会被视为可更新对象
并移动到 LRU 头（`cache.go:145`）。随后在旧根快照后执行：

1. `item.update(value,ttl)`：在 `Item.mu` 下替换 value、递增版本、重置
   `expiresAt`，再按新 TTL touch（`item.go:80`）。
2. `updateExpirations(false,...)` 对旧 heap index 调 `heap.Fix`。
3. 重算 cost；更新 metrics；触发更新事件。

例：`a=1h`、`b=2h`：

| 操作 | map | LRU | heap | 通知 |
| --- | --- | --- | --- | --- |
| `Set(a,...,3h)` | `{a,b}`，A 指针不变 | `[A,B]` | `[B@2h,A@3h]` | 无 |
| `Set(b,...,1m)` | `{a,b}`，B 指针不变 | `[B,A]` | `[B@1m,A@3h]` | 约 1m |

第一次更新把旧根延后：新根 `2h` 不早于旧根 `1h`，不通知。第二次把当前根 `2h`
提前到 `1m`，发送通知。测试 `TestConsistency_UpdateTTLNotifications` 用真实
`Set` 而不是手工篡改根来覆盖这一顺序。

### 3.5 `DeleteExpired`

`DeleteExpired` 持写锁，从 heap 根开始，根未过期即停止；根过期则调用
`evict(EvictionReasonExpired, e)`，删除 map、cost、LRU 节点和 heap 节点，然后
重新读取新根（`cache.go:500`-`cache.go:509`）。

例：heap 为 `[E(已过期), L@1h]`，LRU 为 `[L,E]`：

| 阶段 | map | LRU | heap |
| --- | --- | --- | --- |
| 前 | `{e,l}` | `[L,E]` | `[E,L]`，`E.idx=0` |
| 后 | `{l}` | `[L]` | `[L]`，`L.idx=0`，`E.idx=-1` |

遍历只沿最小堆根推进是正确的：heap 中若根未过期，其他有限 TTL 节点也不可能更早
过期；零值根只在没有有限 TTL 条目时出现。

### 3.6 容量驱逐

新插入前，如果 `len(values)>=capacity`，`set` 先驱逐当前 LRU 尾部
（`cache.go:181`-`cache.go:184`），再解析新 TTL、创建新条目。

例：容量 2，已有 `a(NoTTL)`、`b(1h)`，插入 `c(2h)`：

| 阶段 | map | LRU | heap |
| --- | --- | --- | --- |
| 前 | `{a,b}` | `[B,A]` | `[B@1h,A(NoTTL)]` |
| 驱逐后 | `{b}` | `[B]` | `[B]`，`A.idx=-1` |
| 插入后 | `{b,c}` | `[C,B]` | `[B@1h,C@2h]` |

在 `evict` 内部，事件分发 wrapper 被调用时 map/LRU/heap 的删除已经完成；用户
回调随后在独立 goroutine 中执行。见第 7 节。

### 3.7 Cost 超限驱逐

新条目是先加入 map/LRU/heap、先增加 `c.cost`，再循环驱逐 LRU 尾部直到
`c.cost<=maxCost`（`cache.go:201`-`cache.go:206`）。同 key 更新则先
`cost-oldCost+newCost`，再从尾部驱逐（`cache.go:160`-`cache.go:165`）。

例：上限 3，`a` cost 2、`b` cost 1，插入 cost 2 的 `c`：

| 阶段 | cost | map | LRU | heap |
| --- | --- | --- | --- | --- |
| 前 | 3 | `{a,b}` | `[B,A]` | 依到期序，NoTTL 在后 |
| 插入后、驱逐前 | 5 | `{a,b,c}` | `[C,B,A]` | 三个节点 |
| 驱逐后 | 3 | `{b,c}` | `[C,B]` | `[B,C]` 或并列时按 heap 实现排列，A.idx=-1 |

这个超限中间状态只存在于同一个写临界区内部；持有读锁的观察方法不可能看到它。

## 4. `oldExpiresAt`、`heap.Fix` 与后台唤醒

`updateExpirations` 在变更 heap 后读取新根，但它必须接收变更前的根
`oldExpiresAt`（`cache.go:89`-`cache.go:107`）。原因是条件判断比较的是：

- 新根为零值：无需定时器。
- 旧根非零且新根不早于旧根：现有定时器已经足够早，无需重设。
- 旧根为零值，或新根严格早于旧根：新的最近截止时间需要通知后台。

如果在 `item.update()` 或 `item.touch()` 之后才取旧根，旧根和新根相同，就会
把“旧条目的截止时间被提前”误判为没有变化，后台继续等待原来的长定时器。因此
调用点必须按以下顺序：

```text
加 items.mu 写锁
记录 oldExpiresAt = 变更前 heap[0].expiresAt
修改 Item.expiresAt
heap.Push 或 heap.Fix
读取 newExpiresAt = heap[0].expiresAt
比较并写 timerCh
释放 items.mu
```

`heap.Fix` 的位置由 `Item.queueIndex` 提供。`Swap` 会同步更新两个条目的 index
（`expiration_queue.go:61`），`heap.Remove` 通过旧 index 删除并在 `Pop` 中把被删
条目标为 `-1`。没有 index，更新 TTL 后只能在线性结构里查找；有 index，单次
`Fix` 可以恢复最小堆顺序。

`timerCh` 容量为 1。通知不是无条件覆盖当前 timer，而是：

- 如果通道已有待读 duration，尝试排空；若后台恰好在同一瞬间取走，则 `select`
  的 default 分支接受这种竞争（`cache.go:118`-`cache.go:129`）。
- 如果排到一个更短的旧 duration，则保留更短值。
- 然后写入新建议值。

### 调换“根变化”和“通知”的顺序

设想把顺序改成：

```text
错误顺序：用旧 heap 计算 d=time.Until(oldRoot) 并通知 timer
          然后才修改 item.expiresAt 并 heap.Fix
```

这不会让有限 TTL 条目被提前删除：真正删除时仍会再次执行
`item.isExpiredUnsafe()`，而旧 heap 的根截止时间只会更早或相同。但它可能造成
两种错误：

- 延迟过期：旧根为 1h，更新把同一条目改成 1m，却先按旧 heap 发出 1h。若该通知
  被等待中的后台接收，清理要等到 1h 后才发生；随后的 `DeleteExpired` 会发现条目
  早已过期，但已经晚了。
- 提前等待/空唤醒：旧根已经被删除或延后，而通知仍携带旧短 duration。后台会提前
  醒来跑一次 `DeleteExpired`，结果没有可删项，再按新根重设 timer。

当前实现先完成 heap 变更，再以新根决定通知，因此正常路径避免了延迟过期；旧的
待取通知最多造成一次提前空唤醒。

### `Start` 循环的真实时序

`Start` 在 `stopMu` 下把 `stopped=false`，然后按当前 heap 根创建 timer
（`cache.go:680`-`cache.go:712`）。循环处理三类事件：

- `stopCh`：退出，`Stop` 才把 `stopped=true`。
- `timerCh`：停止并 drain 旧 timer，再 `Reset(d)`。
- `timer.C`：持写锁执行 `DeleteExpired`，然后重新读取当前 heap 根并 Reset。

`Start` 未运行时，`timerCh` 仍可缓存一次建议；之后 `Start` 初始化 timer 后进入
select。Go 的 select 在多个 case 就绪时伪随机选择，因此若初始 timer 也已到期，
测试不能假定一定先消费 `timerCh`。最小可靠实验是放入一个已过期 item，并把
`timerCh<-0` 预先放入队列：无论选择哪个就绪 case，都会调用 `DeleteExpired`，
回调 barrier 关闭即可确定性证明清扫发生。

## 5. TTL 边界和 Stop 前后

### `TTL=0`

`Set(..., DefaultTTL)` 先把 0 替换成 cache 级 `options.ttl`（`cache.go:140`-
`cache.go:143`）。因此公开 `Set` 无法把 0 原样存入：

- 全局 TTL > 0：新条目按全局正 TTL 设置有限 `expiresAt`。
- 全局 TTL 为 0 或负值：`Item.ttl<=0`，`touchUnsafe` 不设置 `expiresAt`
  （`item.go:118`-`item.go:124`）。

### 负 TTL

`NoTTL=-1` 是文档化常量；内部只检查 `ttl<=0`，所以 `-2` 以外的任意负 duration
在进入 `Item` 后也表现为永不过期。`-2` 是 `PreviousOrDefaultTTL` sentinel，只在
内部 TTL 选择阶段被处理，不应作为普通 `Item.ttl` 暴露。

更新同 key 时：

- `Set(k,v,NoTTL)` 会把正 TTL 改成永不过期，并把 `expiresAt` 清零。
- `Set(k,v,PreviousOrDefaultTTL)` 不调用 `touchUnsafe`，保留原 `ttl` 和
  `expiresAt`（`item.go:92`-`item.go:99`）。

### 已经过期但尚未清扫

过期不会主动从 map 中消失。此时：

- 物理上：仍在 `values`、`lru`、`expQueue`，`queueIndex>=0`。
- 逻辑上：`Get`、`Has`、`Items`、`Keys`、`Len`、`Range` 都按当前 wall clock
  过滤它。
- cost：若启用 maxCost，`Cost` 仍计入该物理条目，因为它尚未被 evict。
- 同 key `Set`：复用该 item 指针并更新 TTL，可把逻辑过期对象恢复成可见。

### Stop 前后

`New` 默认 `stopped=true`；不启动后台 goroutine 时，过期条目只能手动清扫。
`Stop` 只通过 unbuffered `stopCh` 通知 `Start` 退出并更新状态，不删除任何 item、
不排空 timerCh、不等待用户事件回调。Stop 后仍可正常读写和手动 `DeleteExpired`。

如果在 `go c.Start()` 的 goroutine 实际运行前调用 `Stop`，由于 `stopCh` 无缓冲且
`stopped` 仍为 true，`Stop` 是 no-op；之后 Start 仍可能运行并泄漏。代码已经在
`IsStarted` 注释中明确提醒这个启动竞态（`cache.go:666`-`cache.go:670`）。

## 6. 观察方法到底承诺什么

### `Items`

`Items` 全程持有 `items.mu.RLock()`，复制的是 `map[K]*Item` 的键和指针，并过滤
调用当下检查为过期的条目（`cache.go:583`-`cache.go:598`）。它的最小保证是：

- map 的成员结构来自某个没有写者提交的读临界区；写者不能在遍历中删除或插入节点。
- 不会 touch、不会移动 LRU、不会 `heap.Fix`、不会清扫。
- 返回的是浅拷贝：map 是新的，但 value 是同一个可变 `*Item`。后续 `Set` 同 key
  时也通常复用该指针，调用者通过旧 map 仍能读到后续值。
- 不复制 `Item.value`，所以不承诺 value/TTL/ExpiresAt 的字段级深快照。

还有一个 wall clock 细节：遍历 map 时每个条目都单独调用
`time.Now().Before` 风格判断，而不是在 RLock 后采样一次统一 now。跨截止边界的极
短时间窗口内，返回集合可能不对应某一个单一逻辑时间点；但它永远不会与写操作产生
撕裂的 map 结构。

### `Keys`

`Keys` 同样全程 RLock，返回新建 slice 和 key 的值拷贝，不过滤物理删除，只过滤
逻辑过期。它不 touch、不清理。它与 `Items()` 分别调用时不承诺同一集合。

### `Len`

`Len` 全程 RLock，但它不是 `len(values)`，而是 BFS heap 统计“当前判断未过期”的
数量（`cache.go:522`-`cache.go:566`）。过期判断在 BFS 中多次读取 wall clock：

- root 未过期时立即认为没有过期节点；在最小堆中这是有效剪枝。
- root 过期后继续向下 BFS，只把检查到过期的节点计数。

因此它不 touch、不清理，也不保证和同时概念上的 `Items`/`Cost` 是同一时刻。

### `Cost`

`Cost` 全程 RLock 并返回 `c.cost`（`cache.go:656`-`cache.go:664`）。未配置
maxCost 时维护逻辑关闭，值始终为 0；配置后，它描述物理存储成本，逻辑过期但尚未
evict 的条目仍计入。它与 `Len()` 的差异不是竞态 bug，而是一个按物理结构计数、
一个按逻辑未过期过滤。

### `Metrics`

`Metrics` 只使用独立 `metricsMu`（`cache.go:648`-`cache.go:654`），不获取
`items.mu`。因此它可以与 cache 结构修改并发，返回计数器值拷贝。它不承诺：

- 与 `Items`、`Keys`、`Len`、`Cost` 属于同一逻辑状态。
- 指标增量和结构变更对外表现为同一个不可分割的复合操作。

例如写路径中 metrics 更新与 map/LRU/heap 修改不是同一把锁；测试只能断言某个历史
区间后的最终计数器，不能把 `Metrics()` 当作事务快照的一部分。

### `Range` / `RangeBackwards`

这两个方法在节点之间允许写者提交：

```text
RLock; 读取当前 elem; 判断 expired; RUnlock
锁外执行 fn(elem)
再次 RLock; elem.Next/Prev; 判断链表是否结束
```

所以它们不是线性一致的全缓存快照。每个“节点 + 回调”是单独的观察点，但整个 Range
可以跨多个已提交状态。链表元素在 Go `container/list` 中保留 prev/next 指针，使
遍历者能从已移动或已删除的节点继续寻找下一个节点；代码还在重新加锁后检查当前
list 长度和边界，避免在 `DeleteAll` 后无限遍历。

`Range` 不 touch。回调在 `items.mu` 外执行，可以安全调用 cache 的公开方法；但是
回调持有的 `*Item` 不是字段快照。

### 回调在锁内还是锁外看到 Item

需要区分两层函数：

1. 内部 wrapper：`set`/`evict` 持有 `items.mu`，并持有事件 map 的 RLock 遍历
   wrapper。插入/更新 wrapper 就在这时启动 goroutine；驱逐 wrapper 在
   map/LRU/heap 都删除后启动 goroutine。
2. 用户注册函数：实际 `fn(ctx,item)` 在新 goroutine 中执行，不持有 `items.mu`、
   `metricsMu` 或 `Item.mu`（`cache.go:772`-`cache.go:777`、
   `cache.go:848`-`cache.go:853`）。

因此用户回调可以调用 cache 方法，不会因为 `items.mu` 自死锁；但它拿到的不是事件
发生时的深拷贝。插入/更新回调可能在稍后才运行，读到同一 `Item` 的后续更新；驱逐
回调看到的是被驱逐 item 上仍可通过 `Item.mu` 安全读取的字段，但该 item 已从三份
cache 结构移除。取消订阅会先从事件 map 删除，再 `WaitGroup.Wait()` 等待已经启动
的用户回调完成；不会等待取消后才发生的新事件。

## 7. 两个并发时序

### 7.1 一次可能的时序：单个 `RangeBackwards` 跨三个状态

容量为 4，初始按顺序插入：`evicted(1h)`、`a(1h)`、`renewed(1h)`、
`replaced(1h)`。初始 LRU 头到尾是：

```text
replaced -> renewed -> a -> evicted
```

`RangeBackwards` 从尾部开始：

1. 观察 `evicted`，记录其旧值；它在回调 barrier 前完成。
2. 观察 `a`，回调进入屏障并释放 `items.mu`。
3. 写者执行 `Get(renewed)`：renewed 被续期并移动到 LRU 头。
4. 写者执行 `Set(replaced,"new")`：复用指针、更新值并移动到 LRU 头。
5. 写者执行 `Set(incoming,...,2h)`：容量满，先驱逐 LRU 尾 `evicted`，再插入
   incoming。
6. barrier 释放；同一 RangeBackwards 继续，随后观察到 renewed 的新到期时间、
   replaced 的新值和新插入 incoming。

这个观察不对应任何一个全局状态：早期回调看见尚未驱逐的 evicted，后续回调又看见
incoming；任何单一持有 RLock 的 `Items()` 都不可能同时包含 evicted 和 incoming。
对应测试是 `TestConsistency_RangeCanSpanMultipleSerializedStates`。

### 7.2 被锁排除的时序：容量驱逐的半截结构

容量为 1，已有 `victim`，写者准备 `Set(incoming)`：

```text
写者: 加 items.mu.Lock
写者: evict(victim)
      delete(map,victim)
      lru.Remove(victim)
      heap.Remove(victim)
      wrapper 启动异步用户回调
写者: PushFront(incoming)
写者: map[incoming]=elem; heap.Push
写者: 释放 items.mu
```

读者不能在“victim 已删但 incoming 未插入，list/heap 暂时为空”的点得到公开观察，
因为该点完全位于写锁内。测试在内部 wrapper（仍持有写锁）放置屏障，证明此时：

- map 中没有 victim，也没有 incoming；
- LRU 和 heap 长度都是 0；
- victim 的 `queueIndex=-1`。

用户 `OnEviction` goroutine 若稍后才运行，则写锁可能已经释放，incoming 已可见；
这正说明 wrapper 的锁内提交点和用户回调的锁外观察点不能混为一谈。对应测试是
`TestConsistency_CapacityEvictionIsAtomicUnderWriteLock`。

### 7.3 物理上三对象共存时的一次 `Items()` 观察

当 map/LRU/heap 中物理上同时存在：

- `renewed`：已被 `Get` 续期；
- `replaced`：同 key 指针已被替换为新值；
- `pending`：到期但未清扫；

`Items()` 的返回只包含 renewed 和 replaced，不包含 pending。`Len()==2`，但启用
maxCost 后 `Cost()==3`，因为 pending 仍在物理结构中。该结果不表示 cost 或 len
错误，而说明它们观察的集合定义不同。对应测试是
`TestConsistency_ItemsCallExcludesExpiredPendingWhilePhysicalStateHasIt`。

## 8. README/注释证据与兼容影响

未发现“订阅函数在独立 goroutine 执行”这一 README 说法与代码冲突：wrapper 在锁内
启动 goroutine，用户函数本身锁外异步执行。需要使用者注意的是异步回调没有事件
payload 快照。

发现以下需要单列的文档/实现语义差异：

1. README 的启动示例直接 `go cache.Start(); defer cache.Stop()`（`README.md:44`-
   `README.md:45`），但 `Stop` 不保证等待一个尚未调度的 Start；`IsStarted` 注释
   明确指出 Stop 可能先发生（`cache.go:666`-`cache.go:670`）。这是启动编排竞态，
   不是 item 状态一致性问题。兼容地修复示例应等待 `IsStarted()==true` 后再 defer
   Stop；改变 Stop 语义（例如等待 Start）可能影响现有调用方，需要新增同步状态。
2. `Items` 注释写的是 “copy of all items”（`cache.go:583`），实际过滤已逻辑过期
   条目，且只复制 map 和指针，不深拷贝 `Item` 或 value。调用方若依赖深拷贝，会在
   后续同 key `Set` 后从旧 map 读到新值；改成深拷贝会改变指针身份、版本和回调
   关联，属于不兼容 API 变化。
3. `Cost` 注释说“all items currently stored”（`cache.go:656`），实际包含逻辑过期
   但未清扫条目；这与 `Len()`/`Items()` 的“unexpired”过滤不同。不应为了对齐名称
   而在 Cost 中顺手清理，否则会引入写操作和回调副作用。
4. `Metrics` 没有使用 items 锁，不与缓存结构形成复合快照。若未来把所有计数器更新
   和结构修改合并到 items 锁，可能降低吞吐并改变回调中观察到的指标顺序。

## 9. 最小可观测保证

在不修改产品代码、不假设可注入时钟的前提下，可以承诺和验证：

- 单个持有 `items.mu.RLock()` 的公开调用（`Items`、`Keys`、`Len`、`Cost`）不会
  观察到写临界区中的半截 map/LRU/heap 结构。
- 这些方法不会 touch，也不会触发删除/驱逐回调；它们只是在 wall clock 上过滤过期。
- `Items`/`Keys` 的容器是新建的，但元素是 key 值或 `*Item` 指针，不是深快照。
- `Range` 的每个回调是一个锁外观察点；整个 Range 可以跨越多个提交状态。
- `Metrics` 是独立计数器快照；与 item 集合没有联合线性一致点。
- 过期未清扫是“逻辑不可见、物理仍计数”的状态；只有 `DeleteExpired`、后台清扫、
  删除或其他驱逐才会改变物理结构。
- 用户事件回调是异步、锁外、无 payload 快照的；需要字段快照时，应由调用方在回调
  中通过 accessor 读取并自行复制。

## 10. 测试索引

新增测试均在 `consistency_analysis_test.go`，并且与产品代码同包，以便精确检查
未导出的 LRU、heap 和 index：

- `TestConsistency_SetReplacementAndFreshPath`：新 `NoTTL` 条目仍入 heap；同 key
  更新复用指针、`heap.Fix` 与通知。
- `TestConsistency_TouchAndNoTouchPaths`：命中 touch 与 disable touch 的 LRU、
  heap、通知差异。
- `TestConsistency_UpdateTTLNotifications`：根延后不通知，根提前通知。
- `TestConsistency_DeleteExpiredPath`：过期根删除、index 置 -1、新根提升。
- `TestConsistency_CapacityEvictionPath`：容量驱逐 LRU 尾及三结构同步移除。
- `TestConsistency_CostEvictionPath`：新 item 先入结构和 cost，再按 LRU 尾降 cost。
- `TestConsistency_TTLBoundaries`：TTL=0、任意负值、过期未清扫、同 key 恢复。
- `TestConsistency_BackgroundCleanupViaBarrierWithoutSleep`：后台循环通过就绪事件
  清扫，不依赖 sleep。
- `TestConsistency_StopDoesNotSweep`：Stop 前后 expired-pending 状态。
- `TestConsistency_ObservationAPIsDoNotTouchOrSweep`：Items/Keys/Len/Cost/Metrics/
  Range 的 touch、清扫和 cost 边界。
- `TestConsistency_CapacityEvictionIsAtomicUnderWriteLock`：锁内 wrapper 看到删除
  已完成、新插入尚未开始；验证半截状态被锁排除。
- `TestConsistency_RangeCanSpanMultipleSerializedStates`：一次 RangeBackwards 同时
  包含旧状态中的被驱逐条目和新状态中的续期、替换、插入条目。
- `TestConsistency_ItemsCallExcludesExpiredPendingWhilePhysicalStateHasIt`：物理三
  对象共存时 Items/Len 与 Cost 的不同集合定义。
- `TestConsistency_ReadLockBarrierExcludesWriter`：读锁屏障内写者无法提交。
- `TestConsistency_InsertionCallbackCanReadLaterState`：插入回调无 payload 快照，
  可读到后续更新。
- `TestConsistency_EvictionCallbackRunsOutsideItemsLock`：用户驱逐回调锁外运行，可
  重入 cache 公开方法。

验证命令：

```sh
go test ./... -count=1 -race
```
