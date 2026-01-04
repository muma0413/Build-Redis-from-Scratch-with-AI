package org.muma.mini.redis.command.impl.zset;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.muma.mini.redis.common.RedisData;
import org.muma.mini.redis.common.RedisZSet;
import org.muma.mini.redis.protocol.*;
import org.muma.mini.redis.store.StorageEngine;
import org.muma.mini.redis.store.impl.MemoryStorageEngine;
import org.muma.mini.redis.store.structure.impl.zset.SkipListZSetProvider;
import org.muma.mini.redis.store.structure.impl.zset.ZipListZSetProvider;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

class ZSetCommandIntegrationTest {

    private StorageEngine storage;
    private ZAddCommand zAdd;
    private ZRangeCommand zRange;
    private ZRevRangeCommand zRevRange;

    // 对应 RedisZSet 中的常量配置
    private static final int MAX_ZIPLIST_ENTRIES = 128;
    private static final int MAX_ZIPLIST_VALUE = 64;

    @BeforeEach
    void setUp() {
        storage = new MemoryStorageEngine();
        zAdd = new ZAddCommand();
        zRange = new ZRangeCommand();
        zRevRange = new ZRevRangeCommand();
    }

    // --- 辅助方法 ---
    private RedisArray args(String... args) {
        RedisMessage[] msgs = new RedisMessage[args.length];
        for (int i = 0; i < args.length; i++) msgs[i] = new BulkString(args[i]);
        return new RedisArray(msgs);
    }

    private RedisZSet getZSet(String key) {
        RedisData<?> data = storage.get(key);
        return data != null ? data.getValue(RedisZSet.class) : null;
    }

    // ★★★ 反射黑魔法：获取内部 Provider ★★★
    private Object getProvider(RedisZSet zset) {
        try {
            Field field = RedisZSet.class.getDeclaredField("provider");
            field.setAccessible(true);
            return field.get(zset);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 测试 1: ZADD 基础与排序
     * 验证是否按 Score 升序，Score 相同按 Member 字典序
     */
    @Test
    void testZAddAndOrder() {
        // 乱序插入
        zAdd.execute(storage, args("ZADD", "myzset", "10", "A", "30", "C", "20", "B"), null);

        // 验证 ZipList 内部有序
        RedisArray res = (RedisArray) zRange.execute(storage, args("ZRANGE", "myzset", "0", "-1"), null);
        RedisMessage[] els = res.elements();

        assertEquals(3, els.length);
        assertEquals("A", ((BulkString) els[0]).asString());
        assertEquals("B", ((BulkString) els[1]).asString());
        assertEquals("C", ((BulkString) els[2]).asString());
    }

    /**
     * 测试 2: 结构升级 (由 Member 长度触发)
     * 插入一个 65 字节的 Member -> 触发 ZipList 转 SkipList
     */
    @Test
    void testUpgradeByValueSize() {
        zAdd.execute(storage, args("ZADD", "upgrade_test", "1", "short"), null);

        RedisZSet zset = getZSet("upgrade_test");
        assertTrue(getProvider(zset) instanceof ZipListZSetProvider, "Should be ZipList initially");

        // 构造超长 Member (65 bytes)
        byte[] largeBytes = new byte[MAX_ZIPLIST_VALUE + 1];
        Arrays.fill(largeBytes, (byte) 'X');
        String largeMember = new String(largeBytes, StandardCharsets.UTF_8);

        // 插入超长 Member
        zAdd.execute(storage, args("ZADD", "upgrade_test", "2", largeMember), null);

        // 验证升级
        assertTrue(getProvider(zset) instanceof SkipListZSetProvider, "Should upgrade to SkipList");

        // 验证数据没丢
        RedisArray res = (RedisArray) zRange.execute(storage, args("ZRANGE", "upgrade_test", "0", "-1"), null);
        assertEquals(2, res.elements().length);
        assertEquals("short", ((BulkString) res.elements()[0]).asString());
        assertEquals(largeMember, ((BulkString) res.elements()[1]).asString());
    }

    /**
     * 测试 3: 结构升级 (由 数量 触发)
     * 插入 129 个元素 -> 触发 ZipList 转 SkipList
     */
    @Test
    void testUpgradeByCount() {
        // 插入 128 个元素 (阈值)
        String[] params = new String[2 + 128 * 2];
        params[0] = "ZADD";
        params[1] = "count_test";
        for (int i = 0; i < 128; i++) {
            params[2 + i * 2] = String.valueOf(i);     // score
            params[2 + i * 2 + 1] = "val-" + i;        // member
        }
        zAdd.execute(storage, args(params), null);

        RedisZSet zset = getZSet("count_test");
        assertTrue(getProvider(zset) instanceof ZipListZSetProvider, "Should be ZipList at threshold");

        // 插入第 129 个
        zAdd.execute(storage, args("ZADD", "count_test", "999", "overflow"), null);

        // 验证升级
        assertTrue(getProvider(zset) instanceof SkipListZSetProvider, "Should upgrade to SkipList");
        assertEquals(129, zset.size());
    }

    /**
     * 测试 4: ZRANGE 各种边界与 WITHSCORES
     */
    @Test
    void testZRangeVariants() {
        zAdd.execute(storage, args("ZADD", "z", "1", "a", "2", "b", "3", "c"), null);

        // 1. 正常范围
        RedisArray res1 = (RedisArray) zRange.execute(storage, args("ZRANGE", "z", "0", "1"), null); // a, b
        assertEquals(2, res1.elements().length);
        assertEquals("b", ((BulkString) res1.elements()[1]).asString());

        // 2. 负数索引 (倒数第一到倒数第一 -> 只取最后一个)
        RedisArray res2 = (RedisArray) zRange.execute(storage, args("ZRANGE", "z", "-1", "-1"), null); // c
        assertEquals(1, res2.elements().length);
        assertEquals("c", ((BulkString) res2.elements()[0]).asString());

        // 3. WITHSCORES
        RedisArray res3 = (RedisArray) zRange.execute(storage, args("ZRANGE", "z", "0", "-1", "WITHSCORES"), null);
        // [a, 1, b, 2, c, 3] -> 6 elements
        assertEquals(6, res3.elements().length);
        assertEquals("1", ((BulkString) res3.elements()[1]).asString());
        assertEquals("c", ((BulkString) res3.elements()[4]).asString());
    }

    /**
     * 测试 5: ZREVRANGE (倒序)
     */
    @Test
    void testZRevRange() {
        zAdd.execute(storage, args("ZADD", "z", "1", "a", "2", "b", "3", "c"), null);

        // ZREVRANGE z 0 1 -> c, b (第一名和第二名)
        RedisArray res = (RedisArray) zRevRange.execute(storage, args("ZREVRANGE", "z", "0", "1"), null);

        assertEquals(2, res.elements().length);
        assertEquals("c", ((BulkString) res.elements()[0]).asString()); // 最大分在最前
        assertEquals("b", ((BulkString) res.elements()[1]).asString());
    }
}
