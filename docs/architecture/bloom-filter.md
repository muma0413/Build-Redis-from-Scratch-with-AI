# 🌸 布隆过滤器 (Bloom Filter) 实现深度解析

Mini-Redis 原生内置了 BloomFilter 模块，利用高效的位图操作，以极小的内存代价提供海量数据的**存在性检测**。

## 1. 原理与核心概念

布隆过滤器是一种空间效率极高的概率型数据结构，用于判断**一个元素是否在一个集合中**。

### 1.1 核心思想
它由两部分组成：
1.  **位数组 (Bit Array)**: 一个长度为 `m` 的二进制数组，初始全为 0。
2.  **哈希函数 (Hash Functions)**: `k` 个独立的哈希函数，将输入映射到数组的 `k` 个位置。

### 1.2 操作流程
*   **添加 (Add)**: 将元素通过 `k` 个哈希函数计算出 `k` 个索引，将位数组中对应的位置标记为 `1`。
*   **查询 (Exists)**: 同样计算出 `k` 个索引。
    *   如果有任意一位是 `0` -> 该元素**一定不存在**。
    *   如果所有位都是 `1` -> 该元素**可能存在** (可能误判)。

### 1.3 假阳性 (False Positive)
为什么会误判？
因为位数组的容量有限，不同的元素可能会哈希到相同的位置（哈希碰撞）。当查询一个从未插入过的元素时，它的 `k` 个位可能碰巧被其他元素置为了 `1`。
*   **误判率** 取决于 `m` (位数组大小), `n` (元素数量), `k` (哈希函数个数)。
*   Mini-Redis 默认配置：`m=8192 bits (1KB)`, `k=5`。

### 1.4 适用场景
*   **爬虫去重**: 记录已经爬过的 URL。
*   **垃圾邮件过滤**: 记录黑名单。
*   **缓存穿透保护**: 在查数据库前先查 BloomFilter，拦截绝大多数不存在的 Key。

---

## 2. Mini-Redis 实现细节

我们没有依赖 RedisBloom Module，而是基于 Mini-Redis 强大的 **String (Bitmap)** 基础设施，原生实现了 BloomFilter。

### 2.1 存储结构 (Memory Layout)
底层复用 `RedisDataType.STRING`，数据存储在 `byte[]` 中。

```text
+-------------------+---------------------------+
| Header (8 bytes)  |      Bitmap Body          |
+-------------------+---------------------------+
|  m (int) | k (int)|  10110010... (m bits)     |
+-------------------+---------------------------+
Header: 存储过滤器的元数据（位数 m 和 哈希函数个数 k），确保持久化后参数不丢失。

Body: 实际的位图数据。
```

### 2.2 哈希策略 (Double Hashing)
为了避免计算 k 个独立哈希的高昂 CPU 开销，我们采用了 Kirsch-Mitzenmacher Optimization 算法：
只需计算两个哈希值 hash1 和 hash2，即可模拟出 k 个哈希值：

```java
hash_i = hash1 + i * hash2
position_i = abs(hash_i % m)
```
Hash 1: MurmurHash3 (高性能、分布均匀)

Hash 2: 简单的乘法 Hash (差异化


### 2.3 命令实现 (Command Implementation)

*   **`BF.RESERVE key m k`**:
    *   **功能**: 初始化一个新的布隆过滤器。
    *   **实现**: 创建一个 `RedisData<byte[]>`，头部写入 8 字节元数据（`m` 和 `k`），后续预分配 `(m + 7) / 8` 字节的零填充数组作为位图主体。
*   **`BF.ADD key item`**:
    *   **功能**: 向过滤器中添加元素。
    *   **实现**:
        1.  从 Storage 获取 byte[]，解析 Header 得到 m 和 k。
        2.  调用哈希算法计算 item 的 k 个目标位置。
        3.  使用位运算 `bytes[byteIdx] |= (1 << bitIdx)` 将对应位置为 1。
        4.  如果至少有一位发生了翻转（从 0 变 1），返回 1；否则返回 0。
*   **`BF.EXISTS key item`**:
    *   **功能**: 检查元素是否存在。
    *   **实现**:
        1.  解析 m 和 k。
        2.  计算 item 的 k 个目标位置。
        3.  使用位运算 `(bytes[byteIdx] >> bitIdx) & 1` 检查每一位。
        4.  **快速失败**: 只要发现任意一位是 0，立即返回 0（一定不存在）。
        5.  如果循环结束所有位都是 1，返回 1（可能存在）。


### 2.4 持久化与复制 (Persistence & Replication)

得益于 Mini-Redis 的分层架构设计，BloomFilter 模块不需要编写任何额外的持久化逻辑，因为它在存储层表现为普通的 **String (byte[])**。

*   **RDB (快照)**:
    *   在生成 RDB 时，BloomFilter 被视为普通的 String 类型。
    *   序列化器直接将底层的 `byte[]`（包含 Header 和位图数据）完整写入磁盘。
    *   加载时，原样恢复为字节数组，逻辑完全透明。
*   **AOF (日志)**:
    *   `BF.ADD` 和 `BF.RESERVE` 被标记为写命令 (`isWrite=true`)。
    *   命令被原样追加到 AOF 文件中。
    *   重启重放时，重新执行哈希计算和置位操作，保证最终状态的一致性 (Deterministic)。
*   **Replication (主从复制)**:
    *   Master 执行 `BF.ADD` 后，将该命令传播给 Slave。
    *   Slave 执行相同的计算逻辑，维护自己本地的位图副本。

---

## 3. 使用示例 (Usage)

您可以通过 `redis-cli` 或 ARDM 等客户端直接操作：

```bash
# 1. 创建自定义过滤器 (容量 m=100 bits, 哈希函数 k=3)
# 注意：如果 m 不是 8 的倍数，会自动向上取整分配字节
BF.RESERVE mybloom 100 3
OK

# 2. 添加元素 "user:1"
BF.ADD mybloom "user:1"
(integer) 1

# 3. 再次添加相同元素 (幂等性)
BF.ADD mybloom "user:1"
(integer) 0

# 4. 检查存在的元素
BF.EXISTS mybloom "user:1"
(integer) 1

# 5. 检查不存在的元素
BF.EXISTS mybloom "user:999"
(integer) 0

# 6. (Hack) 查看底层存储结构
# 可以看到这是一个普通的 String，长度为 8 (Header) + 13 (100 bits) = 21 bytes
STRLEN mybloom
(integer) 21
```

