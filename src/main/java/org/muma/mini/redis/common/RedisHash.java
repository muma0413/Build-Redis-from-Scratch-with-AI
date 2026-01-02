package org.muma.mini.redis.common;

import org.muma.mini.redis.store.structure.HashProvider;
import org.muma.mini.redis.store.structure.impl.hash.HashTableProvider;
import org.muma.mini.redis.store.structure.impl.hash.ZipListProvider;

import java.util.Map;

public class RedisHash {
    // 阈值定义 (模拟 Redis 默认值)
    private static final int ZIPLIST_MAX_ENTRIES = 512;
    private static final int ZIPLIST_MAX_VALUE = 64;

    private HashProvider provider;

    public RedisHash() {
        // 默认使用 ZipList
        this.provider = new ZipListProvider();
    }

    /**
     * ① field 数 ≤ 512；② 每个 field/value 长度 ≤ 64B；
     * 超过上述任一条件，自动转为哈希表；
     * @param field
     * @param value
     * @return
     */
    public int put(String field, byte[] value) {
        // 1. 检查 Value 长度导致的升级
        if (provider instanceof ZipListProvider && value.length > ZIPLIST_MAX_VALUE) {
            convertToHashTable();
        }

        int res = provider.put(field, value);

        // 2. 检查元素数量导致的升级
        if (provider instanceof ZipListProvider && provider.size() > ZIPLIST_MAX_ENTRIES) {
            convertToHashTable();
        }
        return res;
    }

    public byte[] get(String field) {
        return provider.get(field);
    }

    public int remove(String field) {
        return provider.remove(field);
    }

    public int size() {
        return provider.size();
    }

    // 私有方法：升级编码
    private void convertToHashTable() {
        // System.out.println("Converting ZipList to HashTable...");
        Map<String, byte[]> data = provider.toMap();
        this.provider = new HashTableProvider(data);
    }

    // --- 【本次补全的方法】 ---
    /**
     * 获取全量数据 (用于 HGETALL 等命令)
     * 委托给底层 Provider 实现
     */
    public Map<String, byte[]> toMap() {
        return provider.toMap();
    }
}
