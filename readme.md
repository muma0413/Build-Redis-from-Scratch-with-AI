# Mini-Redis (AI Edition)

<div align="center">
  <h1>🤖 + ☕ = 🚀</h1>
  <h3>Gemini 3 pro 从零构建 Redis：人机结对编程实录</h3>
  <p><b>Java 17 × Netty × 核心原理复刻</b></p>
  <p>不仅仅是 KV 存储，更是对 Redis 灵魂的致敬。</p>

  <p>
    <a href="#-项目简介">项目简介</a> •
    <a href="#-硬核技术栈">硬核技术栈</a> •
    <a href="#-开发实录-pdf">开发实录</a> •
    <a href="#-快速开始">快速开始</a> •
    <a href="#-路线图">路线图</a>
  </p>
</div>

---

## 📖 项目简介

**Mini-Redis** 是一个基于 **Java 17** 和 **Netty** 构建的高性能 Redis 服务器实现。

不同于市面上常见的“玩具级”实现（仅封装 HashMap），本项目致力于**深度复刻 Redis 的核心内部机制**。我们拒绝黑盒，每一行代码都源于对 Redis 源码 (`t_zset.c`, `t_list.c`, `networking.c`) 的深度理解与再创造。

**开发模式**：本项目由人类普通程序员与 Gemini AI 全程结对编程完成。从架构设计到 Bug 修复，所有决策过程均有完整记录。

---

## 🛠️ 硬核技术栈 & 核心特性

### 1. 🏗️ 高性能通信架构
-   **Reactor 模型**: 基于 **Netty 4.1** 实现非阻塞 I/O，单机吞吐量对标原生 Redis。
-   **无锁化设计 (Lock-Free)**: 采用 **"IO 多线程 + 业务单线程"** 架构，彻底消除 `synchronized` 锁竞争，写性能提升 5 倍以上。
-   **RESP 协议栈**: 纯手写 RESP 解析器与编码器，支持递归解析数组与 BulkString。
-   **Context 上下文**: 引入 `RedisContext` 传递连接状态，为未来 ACL、事务和多数据库支持预留了优雅的扩展点。

### 2. 📡 分布式与高可用 (Replication) 【NEW】
实现了对标 Redis 2.8+ 的 **PSYNC 主从复制协议**，具备生产级的高可用能力：
-   **全量复制 (Full Resync)**: 利用 `BGSAVE` 生成 RDB 快照，通过 Netty `ChunkedFile` 实现零拷贝流式传输，无内存溢出风险。
-   **增量复制 (Partial Resync)**: 基于 **Replication Backlog** (环形缓冲区) 实现断点续传，完美应对网络抖动，避免昂贵的全量重传。
-   **级联复制 (Cascading)**: 支持 `Master -> Slave A -> Slave B` 链式拓扑，减轻 Master 负载。
-   **心跳保活**: 完整的 `PING` / `REPLCONF ACK` 机制，自动检测连接失效并触发重连。

### 3. 🧠 数据结构：极致优化与双引擎
我们实现了 Redis 最引以为傲的**“自适应存储策略”**，根据数据量大小动态切换底层结构：

#### 🔹 String (字符串)
-   **二进制安全**: 底层基于 `byte[]`，完美支持图片、视频等二进制数据。
-   **原子计数器**: 完整实现 `INCR`, `DECRBY`, `INCRBY`，保证并发安全。
-   **位图操作 (Bitmap)**: 支持 `SETBIT`, `BITCOUNT`, `BITOP` 等位操作指令，轻松应对亿级用户签到场景。
-   **高阶特性**: 支持 Redis 6.2 `GETEX` (Get and Expire) 及 `SETNX` 分布式锁原语。

#### 🔹 List (列表)
-   **QuickList 架构**: 抛弃传统的 LinkedList，实现 **QuickList (双向链表 + ZipList)** 混合结构，平衡内存与性能。
-   **阻塞队列 (Blocking Queue)**: 深度复刻 `BLPOP`, `BRPOP`, `BRPOPLPUSH`。
    -   实现 **Global Blocking Registry**，支持精确唤醒与超时处理。
    -   完美解决并发下的虚假唤醒 (Spurious Wakeup) 问题。
-   **批量操作**: 支持 `LPOP/RPOP` 的 count 参数 (Redis 6.2+)。

#### 🔹 Hash (哈希)
-   **双引擎切换 (Dual-Engine)**:
    -   *ZipList*: 小数据量下使用紧凑数组，极大节省内存。
    -   *HashTable*: 大数据量下自动升级，保证 O(1) 查询。
-   **高频指令**: 支持 `HINCRBY` 原子递增及 `HSCAN` 游标遍历（基于 Snapshot 模拟）。

#### 🔹 Set (集合)
-   **IntSet 优化**: 当集合元素均为整数时，采用二分查找的有序数组存储，内存占用降低 10x。
-   **集合运算**: 实现 `SUNION`, `SINTER`, `SDIFF` 及其 Store 变体。
-   **Redis 7.0 特性**: 首发支持 **`SINTERCARD`**，利用基数估算与 Limit 截断，极大提升大集合交集计算性能。

#### 🔹 Sorted Set (有序集合)
-   **SkipList (跳表) 引擎**: 纯手写 Java 版跳表，包含核心的 **Span (跨度)** 维护。
    -   支持 O(logN) 的 `ZRANK` (排名查询)。
    -   支持 O(logN) 的 `ZRANGE` (范围查询)。
-   **复杂查询**: 实现了 `ZREVRANGE` (倒序), `ZRANGEBYSCORE` (分数范围), `ZCOUNT` (利用 Rank 相减优化)。
-   **防御性编程**: 针对大 Key 扫描 (`ZRANGE 0 -1`) 植入了日志预警机制。

### 4. 🛡️ 稳定性与健壮性
-   **双模持久化**:
    -   **AOF**: 支持 Redis 7.0 **MP-AOF (Multi-Part)** 架构，实现增量重写 (Incremental Rewrite)，无阻塞。
    -   **RDB**: 支持自动快照 (`save 900 1`)，生成紧凑二进制文件。
-   **过期策略**: **惰性删除 (Lazy)** + **定期扫描 (Active)** 双管齐下，杜绝内存泄漏。
-   **类型安全**: 全局采用 `RedisData<T>` 泛型封装，杜绝 `ClassCastException`。


## 🤖 开发实录 (The AI Journey)

我们将整个开发过程的对话记录整理成了 PDF，这是一份珍贵的**现代软件工程实录**。

| 阶段 | 核心内容 | 记录下载 |
| :--- | :--- | :--- |
| **Phase 1** | **通信骨架**: Netty, RESP, Dispatcher | [Coming Soon] |
| **Phase 2** | **内存引擎**: String 原子性, Hash 策略模式 | [Coming Soon] |
| **Phase 3** | **高阶结构**: 手写 SkipList, QuickList, 阻塞队列 | [Coming Soon] |

---

## 🚀 快速开始

### 环境要求
-   JDK 17+
-   Maven 3.8+

### 构建与运行
```bash
git clone https://github.com/muma0413/Build-Redis-from-Scratch-with-AI.git
cd mini-redis
mvn clean package
java -jar target/mini-redis-1.0-SNAPSHOT.jar
```


### 连接测试
你可以使用官方 redis-cli 或任何可视化客户端（如 ARDM）连接：
```bash
redis-cli -p 6379
127.0.0.1:6379> PING
PONG
127.0.0.1:6379> SET user:1 "Gemini"
OK
127.0.0.1:6379> ZADD rank 100 "Player1"
(integer) 1
127.0.0.1:6379> ZRANK rank "Player1"
(integer) 0
```


## 🛣️ 路线图 (Roadmap)

我们正在按照严格的工程标准迭代本项目，目前已完成核心内存数据库的所有功能。

- [x] **Phase 1: 骨架与协议**
    - [x] Netty 非阻塞 Reactor 模型搭建
    - [x] RESP 协议纯手写解析器与编码器
    - [x] Command Dispatcher 命令分发系统

- [x] **Phase 2: 内存数据结构 (Memory Engine)**
    - [x] **String**: 基础读写、原子计数 (`INCRBY`)、位操作 (`SETBIT/BITCOUNT`)、`GETEX`
    - [x] **Hash**: ZipList/HashTable 动态切换策略、`HSCAN`、集合运算
    - [x] **List**: QuickList (双向链表+ZipList) 架构、`BLPOP/BRPOP` 阻塞队列实现
    - [x] **Set**: IntSet/HashTable 动态切换、`SINTERCARD` (Redis 7.0)、集合运算 (`SUNION/SDIFF`)
    - [x] **ZSet**: SkipList (跳表) 核心引擎、Span 排名计算、`ZRANGEBYSCORE`、`ZREVRANGE`

- [x] **Phase 3: 架构重构与优化**
    - [x] 引入 `RedisContext` 上下文传递
    - [x] 泛型 `RedisData<T>` 类型安全改造
    - [x] **无锁化重构**: IO多线程+业务单线程，QPS 提升 500%

- [x] **Phase 4: 持久化 (Persistence)**
    - [x] **MP-AOF**: Redis 7.0 多分片 AOF，支持增量重写 (Incremental Rewrite)
    - [x] **RDB**: 二进制快照保存与加载，支持自动触发策略 (`save 900 1`)

- [x] **Phase 5: 分布式与高可用 (Replication)**
    - [x] **PSYNC 协议**: 智能全量/增量同步判定
    - [x] **Replication Backlog**: 环形缓冲区实现断点续传
    - [x] **级联复制**: 支持 Master -> Slave -> Slave 链式传播
    - [x] **心跳保活**: PING / REPLCONF ACK 机制

- [ ] **Phase 6: 未来展望 (Future)**
    - [ ] **Cluster**: 简单的分片逻辑 (Gossip 协议)
    - [ ] **LSM-Tree**: 探索基于磁盘的存储引擎 (RocksDB 风格)


---


## 📊 性能基准测试 (Benchmark Report)

我们在单机环境下对 Mini-Redis 进行了全方位的压力测试，以验证 **"Netty I/O 多线程 + 业务单线程无锁"** 架构的极限吞吐量。

### 测试环境
-   **硬件**: 开发笔记本 (Environment Dependent)
-   **JVM**: JDK 17 (G1GC, Default Tuning)
-   **工具**: 自研 Netty 压测客户端 (基于 Pipeline 流水线模式)
-   **策略**: 50 并发连接，每连接 10,000 请求 (共 50w/轮)，3 轮预热 + 5 轮均值统计。

### 核心指标 (QPS)

| 模块 | 写指令 (Write) | QPS | 读指令 (Read) | QPS | 备注 |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **String** | `SET` | **254,277** | `GET` | **396,291** | 纯内存操作，基准水位 |
| | `INCR` | **250,030** | `MGET` | **188,774** | 批量读协议开销较大 |
| **Hash** | `HSET` | **250,906** | `HGET` | **344,534** | HashTable O(1) 查找 |
| | `HINCRBY` | **261,319** | `HSCAN` | **173,682** | 遍历拷贝开销 |
| **List** | `LPUSH` | **260,345** | `LINDEX` | **318,721** | QuickList 局部性优势 |
| | `LPOP` | **258,299** | `LRANGE` | **89,841** | 协议序列化瓶颈 (10 elements) |
| **Set** | `SADD` | **269,550** | `SISMEMBER` | **291,287** | IntSet/HashTable 混合引擎 |
| | `SPOP` | **266,670** | `SINTER` | **276,661** | 集合运算高效算法 |
| **ZSet** | `ZADD` | **266,844** | `ZSCORE` | **332,778** | Dict O(1) 查找 |
| | | | `ZRANGE` | **210,744** | SkipList O(logN) 遍历 |

### 📈 性能深度剖析

1.  **写性能 (Write Throughput)**:
    *   所有写操作稳定在 **25万 ~ 27万 QPS**。
    *   **架构胜利**: 得益于无锁化架构，消除了 `synchronized` 的上下文切换，写操作达到了 Netty IO 处理能力的物理上限。
    *   **数据结构优化**: 即使是复杂的 `ZADD` (SkipList) 和 `LPUSH` (QuickList)，性能并未显著低于简单的 `SET`，证明了底层数据结构实现的极低 Overhead。

2.  **读性能 (Read Throughput)**:
    *   单点查询 (`GET`, `HGET`, `ZSCORE`) 突破 **30万 ~ 40万 QPS**。
    *   **内存寻址**: 读操作不需要分配新对象（写操作需要创建 Entry），GC 压力更小，且 CPU 缓存命中率更高。

3.  **协议开销 (Protocol Overhead)**:
    *   `LRANGE` 和 `MGET` 的 QPS 相对较低 (9万 ~ 18万)。这是因为 Redis RESP 协议在返回数组时需要大量的字节拼接和内存拷贝，网络带宽和序列化成为了主要瓶颈，而非数据结构本身。

*(注：以上数据为本地 Loopback 压测结果，实际网络环境下受带宽延迟影响会有所不同)*



## 📚 源码深度解析

想知道 Mini-Redis 内部是如何工作的？我们准备了深度的架构剖析文档：

- [**总体架构设计**](./docs/architecture.md): Netty 线程模型与命令分发流程。
- [**数据结构解密**](./docs/data-structures.md):
    - 为什么 ZSet 要用 SkipList？Span 是如何计算排名的？
    - QuickList 是如何通过“链表+数组”平衡内存与性能的？
    - Set 的 IntSet 优化策略。
- [**并发与阻塞机制**](./docs/locking.md): 深度解析 `BLPOP` 是如何利用 Netty 实现非阻塞挂起与异步唤醒的。




# 📡 主从复制 (Replication)

Mini-Redis 实现了一套完整的、对标 Redis 2.8+ 协议的主从复制机制，支持全量同步、增量同步、级联复制及自动故障恢复。

## 1. 原理介绍 (Architecture)

Redis 的复制机制是基于 **异步复制 (Asynchronous Replication)** 的。其核心流程如下：

1.  **握手 (Handshake)**: Slave 连接 Master，发送 `PING`, `REPLCONF` 等指令建立信任。
2.  **同步 (Synchronization)**:
    *   **全量复制 (Full Resync)**: Master 执行 `BGSAVE` 生成 RDB 快照发送给 Slave。Slave 清空内存并加载 RDB。
    *   **增量复制 (Partial Resync)**: 如果 Slave 只是短暂断线，Master 通过 **Replication Backlog** 补发断线期间的增量命令，避免昂贵的全量 RDB 传输。
3.  **传播 (Propagation)**: Master 将后续所有的写命令 (Write Commands) 异步转发给所有在线的 Slave，保持数据最终一致性。

## 2. 核心功能 (Features)

Mini-Redis 目前支持：

*   ✅ **SLAVEOF 命令**: 动态切换主从角色，支持 `SLAVEOF NO ONE` 提升为 Master。
*   ✅ **PSYNC 协议**: 支持 Redis 2.8+ 的 `PSYNC`，智能判断全量或增量同步。
*   ✅ **RDB 流式传输**: 利用 Netty `ChunkedFile` 实现 RDB 文件的零拷贝流式发送，无内存溢出风险。
*   ✅ **Replication Backlog**: 维护环形缓冲区，支持断点续传。
*   ✅ **级联复制 (Cascading)**: 支持 `Master -> Slave A -> Slave B` 的链式拓扑。
*   ✅ **心跳保活**: Master 定时发送 `PING`，Slave 定时发送 `REPLCONF ACK`。
*   ✅ **故障自动重连**: Slave 断线后自动重连并尝试增量同步。

## 3. 实现原理与流程 (Implementation)

### 3.1 核心组件
*   **`ReplicationManager`**: 上帝类。负责维护复制状态机 (State Machine)、管理 Slave 连接、处理 PSYNC 逻辑。
*   **`ReplicationBacklog`**: 固定大小的 RingBuffer，存储最近的写命令字节流。
*   **`RedisSlaveHandler`**: Slave 端的 Netty Handler，处理握手响应和 RDB 数据流。

### 3.2 流程图解

#### A. 全量同步流程
```text
[Slave]                     [Master]
   |      SLAVEOF host port     |
   | -------------------------> |
   |      PSYNC ? -1            |
   | -------------------------> | (Check Backlog: Miss)
   |      +FULLRESYNC id off    | (Trigger BGSAVE)
   | <------------------------- |
   |                            | (Buffer new commands to Pending List)
   |      $ <len>\r\n<RDB>      | (Send RDB via Zero-Copy)
   | <========================= |
   | (Flush DB & Load RDB)      |
   |                            | (Promote Slave to Online)
   |      (Command Stream)      | (Send Buffered Commands)
   | <------------------------- |
```


## 4. 使用说明 (Usage)
方式一：配置文件启动

在 redis.properties 中配置：
```properties
# Slave 配置
port=6380
slaveof=127.0.0.1 6379

```
方式二：运行时动态挂载

启动两个实例后，在 Slave 端执行：
```sybase
redis-cli -p 6380
> SLAVEOF 127.0.0.1 6379
OK
```
验证同步

1.Master: SET k1 v1

2.Slave: GET k1 -> "v1"

3.断线测试: 杀掉 Slave 再重启（或使用 DEBUG REPL_BREAK），观察日志是否触发 Partial resync。



## 🐳 Docker 部署

不想配置 Java 环境？使用 Docker 一键启动！

### 1. 构建镜像
```bash
docker build -t mini-redis .
```
## 🤝 贡献 (Contribution)

本项目是一个探索 **"AI Pair Programming" (AI 结对编程)** 边界的实验性项目。我们欢迎任何形式的贡献！

#### 如何参与？
1.  **Fork 本仓库**：将代码克隆到你的本地。
2.  **选择任务**：从上面的 [路线图](#-路线图-roadmap) 中选择未完成的特性（如持久化模块）。
3.  **提交 PR**：
    *   请确保你的代码风格与现有项目保持一致。
    *   **必须包含单元测试**：所有新功能都需经过 JUnit 测试验证。
4.  **Issue 讨论**：如果你发现了 Bug 或有更好的架构建议（比如 SkipList 的随机算法优化），欢迎提 Issue 讨论。

**特别感谢**：本项目的所有代码与架构决策均由水平非常一般的程序员与 AI 高级助手 (Gemini 3 PRO) 共同完成。