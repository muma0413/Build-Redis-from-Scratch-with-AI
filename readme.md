# Mini-Redis (AI Edition)

<div align="center">
  <h1>🤖 + ☕ = 🚀</h1>
  <h3>从零构建 Redis：人机结对编程实录</h3>
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

**开发模式**：本项目由人类架构师与 Gemini AI 全程结对编程完成。从架构设计到 Bug 修复，所有决策过程均有完整记录。

---

## 🛠️ 硬核技术栈 & 核心特性

### 1. 🏗️ 高性能通信架构
-   **Reactor 模型**: 基于 **Netty 4.1** 实现非阻塞 I/O，单机吞吐量对标原生 Redis。
-   **RESP 协议栈**: 纯手写 RESP (Redis Serialization Protocol) 解析器与编码器，支持递归解析数组与 BulkString。
-   **Command Dispatcher**: 基于单例模式的高效命令分发器，支持 `RedisContext` 上下文传递，为未来 ACL 和 事务预留扩展点。

### 2. 🧠 数据结构：极致优化与双引擎
我们实现了 Redis 最引以为傲的**“自适应存储策略”**，根据数据量大小动态切换底层结构：

#### 🔹 String (字符串)
-   **二进制安全**: 底层基于 `byte[]`，完美支持图片、视频等二进制数据。
-   **原子计数器**: 完整实现 `INCR`, `DECRBY`, `INCRBY`，保证并发安全。
-   **位图操作 (Bitmap)**: 支持 `SETBIT`, `BITCOUNT` 等位操作指令，轻松应对亿级用户签到场景。
-   **高阶特性**: 支持 Redis 6.2 `GETEX` (Get and Expire) 及 `SETNX` 分布式锁原语。

#### 🔹 List (列表)
-   **QuickList 架构**: 抛弃传统的 LinkedList，实现 **QuickList (双向链表 + ZipList)** 混合结构，平衡内存与性能。
-   **阻塞队列 (Blocking Queue)**: 深度复刻 `BLPOP`, `BRPOP`, `BRPOPLPUSH`。
    -   实现 **Global Blocking Registry**，支持精确唤醒与超时处理。
    -   完美解决并发下的虚假唤醒 (Spurious Wakeup) 问题。

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

### 3. 🛡️ 稳定性与健壮性
-   **过期策略**: **惰性删除 (Lazy)** + **定期扫描 (Active)** 双管齐下，杜绝内存泄漏。
-   **类型安全**: 全局采用 `RedisData<T>` 泛型封装，杜绝 `ClassCastException`。
-   **防御性日志**: 对 O(N) 复杂度的命令（如 `KEYS`, `HGETALL`）增加慢查询预警。

---

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
cd mini-redis-ai
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
    - [x] `BlockingManager` 全局阻塞注册表与并发控制

- [ ] **Phase 4: 持久化 (Persistence)**
    - [ ] **AOF (Append Only File)**: 写命令日志追加与重放
    - [ ] **RDB (Snapshot)**: 内存快照序列化与加载

- [ ] **Phase 5: 分布式与高可用**
    - [ ] **Replication**: 主从复制协议 (PSYNC)
    - [ ] **Cluster**: 简单的分片逻辑 (Optional)

---

## 📚 源码深度解析

想知道 Mini-Redis 内部是如何工作的？我们准备了深度的架构剖析文档：

- [**总体架构设计**](./docs/architecture.md): Netty 线程模型与命令分发流程。
- [**数据结构解密**](./docs/data-structures.md):
    - 为什么 ZSet 要用 SkipList？Span 是如何计算排名的？
    - QuickList 是如何通过“链表+数组”平衡内存与性能的？
    - Set 的 IntSet 优化策略。
- [**并发与阻塞机制**](./docs/locking.md): 深度解析 `BLPOP` 是如何利用 Netty 实现非阻塞挂起与异步唤醒的。



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

**特别感谢**：本项目的所有代码与架构决策均由人类架构师与 AI 助手 (Gemini) 共同完成。
