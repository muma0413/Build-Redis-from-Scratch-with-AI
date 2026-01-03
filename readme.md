# Mini-Redis (AI Edition)

<div align="center">
  <!-- 这里之后可以放一个生成的 Logo -->
  <h1>🤖 + ☕ = 🚀</h1>
  <h3>从零构建 Redis：人机结对编程实录</h3>
  <p>基于 Java 17 + Netty，深度复刻 Redis 底层数据结构与核心原理。</p>
  <p><b>人类架构师的设计 × AI 的极致执行</b></p>

  <p>
    <a href="#-项目简介">项目简介</a> •
    <a href="#-核心特性">核心特性</a> •
    <a href="#-开发实录-pdf">开发实录 (PDF)</a> •
    <a href="#-快速开始">快速开始</a> •
    <a href="#-路线图">路线图</a>
  </p>
</div>

---

## 📖 项目简介

**Mini-Redis** 不仅仅是一个简单的 Key-Value 缓存 Demo。这是一个严肃的工程尝试，旨在用 **Java 17** 和 **Netty** 反向工程并重构 Redis 的核心内部机制。

**本项目的最大亮点在于其开发模式：100% AI 结对编程 (AI Pair Programming)。**
项目中的每一行代码、每一个架构决策（例如使用策略模式实现存储引擎动态切换）、每一次 Bug 修复，都源于人类架构师与 Gemini AI 之间的一场场深度对话。

这个仓库承载了两个使命：
1.  作为一个高质量的教学项目，展示 Redis 核心组件（跳表、压缩列表、Reactor模型）的 Java 实现细节。
2.  作为一个**软件工程实验**，展示“人类负责设计与把控，AI 负责编码与落地”的未来开发范式。

## ✨ 核心特性

我们拒绝简单的 `HashMap` 包装，我们追求对 Redis 原理的忠实还原：

### 🏗️ 基础架构
-   **高性能网络**: 基于 **Netty 4.1** 的非阻塞 IO 模型。
-   **协议解析**: 纯手写完整的 **RESP (Redis Serialization Protocol)** 解析器与编码器。
-   **分层存储**: 抽象的 `StorageEngine` 接口，支持底层存储引擎的插件化（为未来接入 LSM-Tree 做准备）。
-   **泛型设计**: 利用 Java 17 泛型特性构建类型安全的 `RedisData<T>` 模型。

### 🧠 数据结构与算法
-   **String**: 二进制安全字符串，支持原子计数 (`INCR`)、分布式锁原语 (`SETNX`, `SET EX NX`)。
-   **Hash**: 实现 **双引擎动态切换 (Dual-Engine Storage)**：
    -   *ZipList (压缩列表)*: 小数据量下使用紧凑布局节省内存。
    -   *HashTable (哈希表)*: 大数据量下自动升级，保证 O(1) 性能。
-   **Sorted Set (ZSet)**: **生产级实现**：
    -   手写 **SkipList (跳表)** 核心引擎，包含 `span` (跨度) 计算，支持 O(logN) 的排名查询 (`ZRANK`)。
    -   结合 `HashMap` 实现 O(1) 的分数查询。
    -   同样支持 ZipList 与 SkipList 的自适应切换。
-   **过期策略**: 实现 **惰性删除 (Lazy)** + **定期删除 (Active)** 策略，防止内存泄漏。

## 🤖 开发实录 (PDF)

相比于代码，**“如何写出代码”的过程**往往更有价值。我们将整个开发过程的对话记录整理成了 PDF，完整还原了从 0 到 1 的思考路径。

*在这里，你可以看到我们是如何讨论架构、如何发现逻辑漏洞、以及 AI 是如何被“调教”成顶级程序员的。*

| 阶段 | 内容描述 | 记录下载 |
| :--- | :--- | :--- |
| **Phase 1** | **通信骨架**: Netty 环境搭建、RESP 协议状态机解析、基础命令分发。 | [下载 PDF](./docs/conversation/phase1-protocol.pdf) |
| **Phase 2** | **基础结构**: 实现 `RedisData<T>`，String 原子性，以及 Hash 的策略模式设计。 | [下载 PDF](./docs/conversation/phase2-hash.pdf) |
| **Phase 3** | **ZSet 与跳表**: 最硬核的一章。手写概率性跳表、维护 Span 指针、实现排名计算。 | [下载 PDF](./docs/conversation/phase3-skiplist.pdf) |

*(注：对话记录将随项目进度持续更新)*

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
