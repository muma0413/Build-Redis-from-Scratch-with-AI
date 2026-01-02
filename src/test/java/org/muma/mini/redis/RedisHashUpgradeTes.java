package org.muma.mini.redis;

import org.junit.jupiter.api.Test;
import org.muma.mini.redis.common.RedisHash;
import org.muma.mini.redis.store.structure.impl.hash.HashTableProvider;
import org.muma.mini.redis.store.structure.impl.hash.ZipListProvider;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

class RedisHashUpgradeTest {

    // 对应 RedisHash 类中的常量定义
    private static final int ZIPLIST_MAX_VALUE = 64;
    private static final int ZIPLIST_MAX_ENTRIES = 512;

    @Test
    void testUpgradeByValueSize() throws Exception {
        // 1. 初始化
        RedisHash hash = new RedisHash();

        // 断言：初始状态应该是 ZipList
        assertTrue(getProvider(hash) instanceof ZipListProvider, "Initial state should be ZipList");

        // 2. 插入一个边界内的数据 (64 bytes)
        byte[] borderValue = new byte[ZIPLIST_MAX_VALUE];
        Arrays.fill(borderValue, (byte) 'a');
        hash.put("k1", borderValue);

        // 断言：仍然是 ZipList
        assertTrue(getProvider(hash) instanceof ZipListProvider, "Should remain ZipList when value <= 64 bytes");
        assertArrayEquals(borderValue, hash.get("k1"));

        // 3. 插入一个超大的数据 (65 bytes) -> 触发升级
        byte[] hugeValue = new byte[ZIPLIST_MAX_VALUE + 1];
        Arrays.fill(hugeValue, (byte) 'b');
        hash.put("k2", hugeValue);

        // 断言：应该升级为 HashTable
        Object currentProvider = getProvider(hash);
        assertTrue(currentProvider instanceof HashTableProvider,
                "Should upgrade to HashTable when value > 64 bytes. Current: " + currentProvider.getClass().getSimpleName());

        // 4. 数据完整性检查 (验证数据在迁移过程中没有丢失)
        assertArrayEquals(borderValue, hash.get("k1"), "Old data should be preserved after upgrade");
        assertArrayEquals(hugeValue, hash.get("k2"), "New huge data should be stored");
    }

    @Test
    void testUpgradeByEntryCount() throws Exception {
        // 1. 初始化
        RedisHash hash = new RedisHash();
        assertTrue(getProvider(hash) instanceof ZipListProvider);

        // 2. 插入 512 个元素 (阈值)
        for (int i = 0; i < ZIPLIST_MAX_ENTRIES; i++) {
            hash.put("key-" + i, ("val-" + i).getBytes(StandardCharsets.UTF_8));
        }

        // 断言：正好卡在阈值，应该是 ZipList
        assertTrue(getProvider(hash) instanceof ZipListProvider,
                "Should remain ZipList when entries <= 512. Current Size: " + hash.size());
        assertEquals(ZIPLIST_MAX_ENTRIES, hash.size());

        // 3. 插入第 513 个元素 -> 触发升级
        hash.put("overflow-key", "overflow-value".getBytes(StandardCharsets.UTF_8));

        // 断言：应该升级为 HashTable
        Object currentProvider = getProvider(hash);
        assertTrue(currentProvider instanceof HashTableProvider,
                "Should upgrade to HashTable when entries > 512. Current: " + currentProvider.getClass().getSimpleName());
        assertEquals(ZIPLIST_MAX_ENTRIES + 1, hash.size());

        // 4. 数据完整性检查 (随机抽查)
        assertNotNull(hash.get("key-0"));
        assertNotNull(hash.get("key-100"));
        assertNotNull(hash.get("key-511"));
        assertEquals("overflow-value", new String(hash.get("overflow-key")));
    }

    // --- 辅助方法：利用反射获取私有的 provider 字段 ---
    private Object getProvider(RedisHash hash) throws Exception {
        // "provider" 必须与 RedisHash 类中定义的字段名一致
        Field field = RedisHash.class.getDeclaredField("provider");
        field.setAccessible(true);
        return field.get(hash);
    }
}
