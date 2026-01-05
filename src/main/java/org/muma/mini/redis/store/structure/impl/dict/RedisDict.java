package org.muma.mini.redis.store.structure.impl.dict;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.muma.mini.redis.util.MurmurHash3;

/**
 * Redis 风格的渐进式 Rehash 字典
 * <p>
 * 【设计目标】
 * 1. 解决 JDK HashMap 扩容时的 Stop-The-World (STW) 问题。
 * 2. 将庞大的扩容开销分摊到每次 CRUD 操作中 (Amortized Cost)。
 * <p>
 * 【核心架构】
 * - 维护两个哈希表：table[0] (旧) 和 table[1] (新)。
 * - 平时只使用 table[0]。
 * - 扩容时，分配 table[1]，并设置 rehashidx = 0。
 * - 每次增删改查时，顺便迁移 rehashidx 指向的 bucket 数据到 table[1]。
 */
public class RedisDict<K, V> implements Dict<K, V> {

    // 初始容量 4，符合 Redis 习惯
    private static final int INITIAL_CAPACITY = 4;

    // 负载因子 1.0 (Redis 倾向于更满一点再扩容，以节省内存)
    private static final float LOAD_FACTOR = 1.0f;

    // 两个哈希表，table[1] 仅在 rehash 时非空
    // 数组大小必须是 2 的幂次方
    @SuppressWarnings("unchecked")
    private Node<K, V>[] ht0 = new Node[INITIAL_CAPACITY];
    @SuppressWarnings("unchecked")
    private Node<K, V>[] ht1 = null;

    // 元素总数
    private int size = 0;

    // Rehash 进度索引。-1 表示未进行 Rehash
    private int rehashIdx = -1;

    // 链表节点
    private static class Node<K, V> {
        final K key;
        V value;
        Node<K, V> next;

        Node(K key, V value, Node<K, V> next) {
            this.key = key;
            this.value = value;
            this.next = next;
        }
    }

    public RedisDict() {
    }

    // --- 核心 API ---

    @Override
    public V get(K key) {
        if (isRehashing()) rehashStep(); // 渐进式迁移一步

        // 1. 先查旧表
        V val = findInTable(ht0, key);
        if (val != null) return val;

        // 2. 如果在 Rehash，还要查新表
        // 注意：如果在旧表没找到，且正在 Rehash，那么可能已经被迁移到新表了
        if (isRehashing()) {
            return findInTable(ht1, key);
        }
        return null;
    }

    @Override
    public V put(K key, V value) {
        if (isRehashing()) rehashStep();

        // 1. 尝试更新旧表
        Node<K, V> node = findNode(ht0, key);
        if (node != null) {
            V oldVal = node.value;
            node.value = value;
            return oldVal;
        }

        // 2. 尝试更新新表 (如果正在 Rehash)
        if (isRehashing()) {
            node = findNode(ht1, key);
            if (node != null) {
                V oldVal = node.value;
                node.value = value;
                return oldVal;
            }
        }

        // 3. 新增节点
        // 如果正在 Rehash，新节点必须直接放入 ht1，保证 ht0 只减不增
        if (isRehashing()) {
            insertToTable(ht1, key, value);
        } else {
            insertToTable(ht0, key, value);
            // 检查是否需要扩容
            checkResize();
        }

        size++;
        return null;
    }

    @Override
    public V remove(K key) {
        if (isRehashing()) rehashStep();

        // 1. 尝试从旧表删除
        V val = removeFromTable(ht0, key);
        if (val != null) {
            size--;
            return val;
        }

        // 2. 尝试从新表删除
        if (isRehashing()) {
            val = removeFromTable(ht1, key);
            if (val != null) {
                size--;
                return val;
            }
        }
        return null;
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public boolean containsKey(K key) {
        return get(key) != null;
    }

    /**
     * 获取所有 Key (用于 HKEYS 等)
     * 注意：这会遍历两个表，如果在 rehash 中
     */
    @Override
    public List<K> keys() {
        List<K> keys = new ArrayList<>(size);
        collectKeys(ht0, keys);
        if (isRehashing()) {
            collectKeys(ht1, keys);
        }
        return keys;
    }

    /**
     * 获取所有 Value
     */
    public List<V> values() {
        List<V> values = new ArrayList<>(size);
        collectValues(ht0, values);
        if (isRehashing()) {
            collectValues(ht1, values);
        }
        return values;
    }

    public List<Map.Entry<K, V>> entries() {
        List<Map.Entry<K, V>> entries = new ArrayList<>(size);
        collectEntries(ht0, entries);
        if (isRehashing()) {
            collectEntries(ht1, entries);
        }
        return entries;
    }

    // --- 内部核心逻辑 ---

    private boolean isRehashing() {
        return rehashIdx != -1;
    }

    /**
     * 检查是否需要扩容
     * 触发条件：size >= capacity * loadFactor
     */
    private void checkResize() {
        if (size >= ht0.length * LOAD_FACTOR) {
            // 扩容为原来的 2 倍
            // 注意：这里没有处理超大容量溢出 int 范围的情况，简化处理
            startRehash(ht0.length * 2);
        }
    }

    @SuppressWarnings("unchecked")
    private void startRehash(int newSize) {
        // System.out.println("DEBUG: Start rehashing to size " + newSize);
        ht1 = new Node[newSize];
        rehashIdx = 0;
    }

    /**
     * 渐进式迁移一步
     * 每次迁移 1 个非空 bucket
     */
    private void rehashStep() {
        // 为了防止某次操作耗时过长，我们限制最大访问空桶数 (例如 10倍)
        // 但这里为了简化，直到找到一个非空桶为止。
        // 如果 bucket 全空，循环会很快。

        // 我们每次只迁移 1 个有效 bucket
        int steps = 1;
        while (steps-- > 0 && rehashIdx < ht0.length) {

            // 跳过空桶
            while (ht0[rehashIdx] == null) {
                rehashIdx++;
                if (rehashIdx >= ht0.length) {
                    finishRehash();
                    return;
                }
            }

            // 迁移当前桶的所有节点
            Node<K, V> node = ht0[rehashIdx];
            while (node != null) {
                Node<K, V> next = node.next;

                // 计算在新表的位置 (Mask = len - 1)
                // 因为 len 是 2 的幂，idx = hash & mask
                int idx = hash(node.key) & (ht1.length - 1);

                // 头插法插入 ht1
                node.next = ht1[idx];
                ht1[idx] = node;

                node = next;
            }
            ht0[rehashIdx] = null; // 释放旧桶引用
            rehashIdx++;
        }

        if (rehashIdx >= ht0.length) {
            finishRehash();
        }
    }

    private void finishRehash() {
        // System.out.println("DEBUG: Rehash finished");
        ht0 = ht1; // 新表变旧表
        ht1 = null;
        rehashIdx = -1;
    }

    // --- 辅助操作 ---

    private int hash(Object key) {
        int h;
        if (key instanceof String) {
            h = MurmurHash3.hash32((String) key);
        } else if (key instanceof byte[]) {
            h = MurmurHash3.hash32((byte[]) key);
        } else if (key instanceof ByteBuffer) {
            // ByteBuffer 需要特殊处理，不能改变 position
            // 或者直接用 JDK hashCode (ByteBuffer.hashCode 是基于内容的)
            // 为了追求极致分布，建议转 byte[] 再 hash，但会有拷贝开销。
            // 鉴于 ByteBuffer.hashCode 已经很不错了，且 Set 底层用的是 ByteBuffer
            // 这里我们暂时保留 JDK hashCode 避免拷贝，或者手写一个针对 ByteBuffer 的 Murmur
            // 这里简单调用 JDK:
            h = key.hashCode();
        } else {
            // 其他类型 fallback 到 JDK hash
            h = key.hashCode();
        }

        // 扰动函数 (Murmur 已经很散了，这步甚至可以省略，但加了更保险)
        return (h ^ (h >>> 16));
    }

    private V findInTable(Node<K, V>[] table, K key) {
        if (table == null) return null;
        int idx = hash(key) & (table.length - 1);
        Node<K, V> e = table[idx];
        while (e != null) {
            if (e.key.equals(key)) return e.value;
            e = e.next;
        }
        return null;
    }

    private Node<K, V> findNode(Node<K, V>[] table, K key) {
        if (table == null) return null;
        int idx = hash(key) & (table.length - 1);
        Node<K, V> e = table[idx];
        while (e != null) {
            if (e.key.equals(key)) return e;
            e = e.next;
        }
        return null;
    }

    private void insertToTable(Node<K, V>[] table, K key, V value) {
        int idx = hash(key) & (table.length - 1);
        // 头插法 (O(1))
        Node<K, V> newNode = new Node<>(key, value, table[idx]);
        table[idx] = newNode;
    }

    private V removeFromTable(Node<K, V>[] table, K key) {
        if (table == null) return null;
        int idx = hash(key) & (table.length - 1);
        Node<K, V> e = table[idx];
        Node<K, V> prev = null;

        while (e != null) {
            if (e.key.equals(key)) {
                if (prev == null) {
                    table[idx] = e.next; // 删除头节点
                } else {
                    prev.next = e.next; // 删除中间/尾节点
                }
                return e.value;
            }
            prev = e;
            e = e.next;
        }
        return null;
    }

    private void collectKeys(Node<K, V>[] table, List<K> list) {
        if (table == null) return;
        for (Node<K, V> node : table) {
            while (node != null) {
                list.add(node.key);
                node = node.next;
            }
        }
    }

    private void collectValues(Node<K, V>[] table, List<V> list) {
        if (table == null) return;
        for (Node<K, V> node : table) {
            while (node != null) {
                list.add(node.value);
                node = node.next;
            }
        }
    }

    private void collectEntries(Node<K, V>[] table, List<Map.Entry<K, V>> list) {
        if (table == null) return;
        for (Node<K, V> node : table) {
            while (node != null) {
                // 使用 SimpleImmutableEntry 或自定义 Entry
                list.add(new java.util.AbstractMap.SimpleImmutableEntry<>(node.key, node.value));
                node = node.next;
            }
        }
    }
}
