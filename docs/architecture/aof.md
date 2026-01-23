# 💾 AOF (Append Only File) 持久化引擎深度解析

Mini-Redis 摒弃了传统的单一 AOF 文件模式，全面拥抱 **Redis 7.0 Multi-Part AOF (MP-AOF)** 架构。这使得我们在享受 AOF 高可靠性的同时，彻底解决了 AOF 重写期间的 I/O 阻塞和内存开销问题。

## 1. 核心原理 (Core Concepts)

### 1.1 为什么需要 MP-AOF？
在旧版 Redis (7.0 之前) 中，AOF 重写是一个重型操作：
1.  **I/O 争抢**: 重写期间产生的增量数据需要写入内存 Buffer (`aof_rewrite_buf`)，这会消耗大量内存。
2.  **复杂性**: 重写完成后，主进程需要将 Buffer 里的数据再刷入新文件，这会造成瞬间的 CPU 和 I/O 峰值。

**MP-AOF 的解法**: 将 AOF 拆分为三种文件角色，物理隔离历史数据与增量数据。

*   **Base AOF**: 基础文件（全量快照）。只读。
*   **Incr AOF**: 增量文件（命令日志）。可写。
*   **Manifest**: 清单文件。负责管理上述文件的生命周期和加载顺序。

### 1.2 架构设计
我们的 Mini-Redis 实现了以下三层架构：

```text
[Command Execution]
       |
       v
[AofManager (Level 2: Logic)] --(Manage Manifest & Rotation)--> [AofManifest]
       |
       v
[AofDiskWriter (Level 1: IO)] --(Async Queue & Fsync)--> [Disk Files]
```

## 2. 实现细节 (Implementation Details)

```text
正常写入流程 (Command Execution & Append)


[Client]
| (SEND command)
v
[Netty IO Thread]
| (Decode RESP)
v
[RedisCommandHandler]
| (Submit Task)
v
[RedisCoreExecutor] (Single Thread)
|
+-> [CommandDispatcher.dispatch(cmd)]
|      |
|      +-> [RedisCommand.execute()]  (执行业务逻辑)
|      |      |
|      |      +-> [StorageEngine.put/remove] (修改内存)
|      |
|      +-> [Check: isWrite() && success?]
|             | (YES)
|             v
|          [AofManager.append(cmd)]
|             |
|             +-> [RespCodecUtil.encode] (序列化)
|             |
|             +-> [AofDiskWriter.write] (非阻塞 Offer)
|             |      |
|             |      v
|             |   [BufferQueue]
|             |
|             +-> [checkRewrite()] (触发检查)
|
v
(Next Command)


 后台刷盘流程 (Background Fsync)

 [AofDiskWriter Thread]
    |
    +-> (Loop: queue.take())
    |      |
    |      +-> [FileChannel.write()] (写入 OS Cache)
    |      |
    |      +-> (IF always) -> [force()]
    |
    +-> [AofFsyncThread] (Scheduled)
           |
           +-> (Every 1s) -> [FileChannel.force()] (落盘)


重写流程 (Rewrite Cycle)

[RedisCoreExecutor]
   |
   +-> [AofManager.checkRewrite()]
          |
          +-> (Condition Met?) -> [triggerRewrite()]
                 |
                 +-> [startNewIncrFile()] (切分新 Incr)
                 |      |
                 |      +-> Update Manifest (Memory)
                 |      +-> Save Manifest (Disk)
                 |
                 +-> [rewriteExecutor.submit(performRewrite)]
                        |
[AofRewriter Thread] <--+
   |
   +-> [performRewrite()]
          |
          +-> [AofRewriter.rewrite(newBase)]
          |      |
          |      +-> [StorageEngine.keys()] (弱一致性遍历)
          |      +-> [objectToCommand()]
          |      +-> [BufferedOutputStream.write()]
          |
          +-> [synchronized (AofManager)]
          |      |
          |      +-> [manifest.setBaseAof()]
          |      +-> [manifest.pruneHistory()]
          |      +-> [saveManifest()]
          |
          +-> [cleanup()] (删除旧文件)
```


### 2.1 三级缓冲写入 (Three-Level Buffering)
为了在保证数据安全的同时实现极致的写性能，我们设计了**无锁异步写入流水线**，将磁盘 I/O 的延迟与主业务线程完全解耦：

1.  **Level 1 (Memory Queue)**:
    *   主线程将写命令序列化后，`offer` 进一个有界的 `ArrayBlockingQueue`。
    *   **优势**: 此操作为纯内存操作，耗时在微秒级，确保**绝不阻塞主业务线程**。
    *   *背压机制*: 如果磁盘写入过慢导致队列满，主线程会进行流控，防止 OOM。

2.  **Level 2 (OS Page Cache)**:
    *   独立的后台 `AOF-Writer` 线程持续从队列消费数据，调用 Java NIO 的 `FileChannel.write`。
    *   数据此时进入操作系统的页缓存 (Page Cache)，虽然尚未落盘，但已不受 JVM 崩溃影响。

3.  **Level 3 (Disk Sync)**:
    *   独立的 `AOF-Fsync` 线程根据配置策略 (`everysec`) 定期执行 `FileChannel.force(false)`。
    *   这确保了数据在物理磁盘上的持久化，即使断电也最多只丢失 1 秒的数据。

### 2.2 增量重写状态机 (Incremental Rewrite State Machine)
这是 MP-AOF 架构的灵魂。我们在 Java 中复刻了 Redis 的 **Pre-Rotate (预先切分)** 策略，消除了传统 AOF 重写所需的复杂内存缓冲区：

1.  **Trigger (触发)**:
    *   当 AOF 文件大小增长达到阈值（如 100% 且 > 64MB）时触发。

2.  **Pre-Rotate (预切分 - 关键步骤)**:
    *   主线程**立即**关闭当前的 `Incr(N)` 文件，创建并打开一个新的 `Incr(N+1)` 文件。
    *   **目的**: 物理隔离重写期间的新写入。所有新产生的写命令直接落入新文件，不再需要额外的内存 Buffer 进行暂存。

3.  **Snapshot (快照)**:
    *   后台 `Rewriter` 线程利用 `ConcurrentHashMap` 的弱一致性迭代器遍历内存数据。
    *   将内存数据转换为精简的 RESP 写命令，写入新的 `Base(New)` 文件。

4.  **Atomic Switch (原子切换)**:
    *   重写完成后，主线程执行原子操作更新 Manifest 文件：
        *   旧状态: `[Base(Old), Incr(N), Incr(N+1)]`
        *   新状态: `[Base(New), Incr(N+1)]`
    *   `Base(Old)` 和 `Incr(N)` 因为数据已包含在 `Base(New)` 中，被安全删除。


## 3. 亮点特性 (Highlights)

*   ✨ **Redis 7.0 协议对齐 (Protocol Alignment)**
    *   生成的文件结构（Manifest 索引 + Base 基准 + Incr 增量）与官方 Redis 完全一致，确保了架构的前瞻性和兼容性。

*   🚀 **Zero-Blocking (零阻塞设计)**
    *   从命令追加到文件轮转，主线程全程无阻塞。得益于异步队列缓冲，即使在磁盘 I/O 剧烈抖动时，QPS 吞吐量依然能保持平稳。

*   🛡️ **Data Safety (数据安全)**
    *   严格遵循 `fsync` 策略。Manifest 文件的更新采用 `Write-Temp-Move` 原子操作，彻底防止因进程崩溃或断电导致的元数据损坏。

*   🔧 **Self-Healing (自愈能力)**
    *   启动时自动检测并清理目录下的僵尸文件（Orphan Files），无需人工干预即可维护磁盘整洁。

---

## 4. 使用与配置 (Usage)

在 `redis.properties` 中启用 AOF 持久化：

```properties
# 开启 AOF 功能
appendonly=yes

# 刷盘策略: always (最安全) | everysec (推荐) | no (最快)
appendfsync=everysec

# AOF 文件存储目录 (相对于工作目录)
appenddirname=appendonlydir

# AOF 重写触发条件
auto-aof-rewrite-percentage=100
auto-aof-rewrite-min-size=64mb
```

目录结构示例

当 AOF 运行一段时间并触发重写后，您的磁盘结构将如下所示：
```text
appendonlydir/
  appendonly.aof.3.base.aof   <-- 历史全量数据 (快照)
  appendonly.aof.2.incr.aof   <-- 当前活跃增量 (日志)
  appendonly.aof.manifest     <-- 索引文件 (记录文件版本)
```

