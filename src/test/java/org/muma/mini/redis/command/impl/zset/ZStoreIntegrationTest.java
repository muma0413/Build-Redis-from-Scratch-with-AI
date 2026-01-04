package org.muma.mini.redis.command.impl.zset;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.muma.mini.redis.common.RedisData;
import org.muma.mini.redis.common.RedisZSet;
import org.muma.mini.redis.protocol.*;
import org.muma.mini.redis.store.StorageEngine;
import org.muma.mini.redis.store.impl.MemoryStorageEngine;

import static org.junit.jupiter.api.Assertions.*;

class ZStoreIntegrationTest {

    private StorageEngine storage;
    private ZAddCommand zAdd;
    private ZUnionStoreCommand zUnion;
    private ZInterStoreCommand zInter;
    // 辅助验证结果
    private ZRangeCommand zRange;

    @BeforeEach
    void setUp() {
        storage = new MemoryStorageEngine();
        zAdd = new ZAddCommand();
        zUnion = new ZUnionStoreCommand();
        zInter = new ZInterStoreCommand();
        zRange = new ZRangeCommand();
    }

    // --- 辅助方法 ---
    private RedisArray args(String... args) {
        RedisMessage[] msgs = new RedisMessage[args.length];
        for (int i = 0; i < args.length; i++) {
            msgs[i] = new BulkString(args[i]);
        }
        return new RedisArray(msgs);
    }

    private void createZSet(String key, Object... scoreMembers) {
        // 构造参数数组: ZADD key score member ...
        String[] params = new String[2 + scoreMembers.length];
        params[0] = "ZADD";
        params[1] = key;
        for (int i = 0; i < scoreMembers.length; i++) {
            params[2 + i] = String.valueOf(scoreMembers[i]);
        }
        zAdd.execute(storage, args(params), null);
    }

    private double getScore(String key, String member) {
        RedisData<?> data = storage.get(key);
        assertNotNull(data);
        RedisZSet zset = data.getValue(RedisZSet.class);
        Double score = zset.getScore(member);
        assertNotNull(score, "Member " + member + " not found in " + key);
        return score;
    }

    /**
     * 测试 1: 基础并集 (Default: Sum, Weight=1)
     * Set1: {A:1, B:2}
     * Set2: {B:3, C:4}
     * Result: {A:1, B:5(2+3), C:4}
     */
    @Test
    void testUnionBasic() {
        createZSet("z1", 1, "A", 2, "B");
        createZSet("z2", 3, "B", 4, "C");

        // ZUNIONSTORE out 2 z1 z2
        RedisMessage res = zUnion.execute(storage, args("ZUNIONSTORE", "out", "2", "z1", "z2"), null);
        assertEquals(3L, ((RedisInteger) res).value());

        // 验证分数
        assertEquals(1.0, getScore("out", "A"));
        assertEquals(5.0, getScore("out", "B")); // 2+3
        assertEquals(4.0, getScore("out", "C"));
    }

    /**
     * 测试 2: 基础交集 (Default: Sum, Weight=1)
     * Set1: {A:1, B:2}
     * Set2: {B:3, C:4}
     * Result: {B:5(2+3)} -> A 和 C 被丢弃
     */
    @Test
    void testInterBasic() {
        createZSet("z1", 1, "A", 2, "B");
        createZSet("z2", 3, "B", 4, "C");

        // ZINTERSTORE out 2 z1 z2
        RedisMessage res = zInter.execute(storage, args("ZINTERSTORE", "out", "2", "z1", "z2"), null);
        assertEquals(1L, ((RedisInteger) res).value());

        // 验证
        assertEquals(5.0, getScore("out", "B"));

        // 验证 A 和 C 不存在
        RedisData<?> data = storage.get("out");
        RedisZSet zset = data.getValue(RedisZSet.class);
        assertNull(zset.getScore("A"));
        assertNull(zset.getScore("C"));
    }

    /**
     * 测试 3: 权重测试 (WEIGHTS)
     * Set1: {A:1, B:2} weight=2 -> {A:2, B:4}
     * Set2: {B:3, C:4} weight=3 -> {B:9, C:12}
     * Union: {A:2, B:13(4+9), C:12}
     */
    @Test
    void testWeights() {
        createZSet("z1", 1, "A", 2, "B");
        createZSet("z2", 3, "B", 4, "C");

        // ZUNIONSTORE out 2 z1 z2 WEIGHTS 2 3
        RedisMessage res = zUnion.execute(storage, args(
                "ZUNIONSTORE", "out", "2", "z1", "z2", "WEIGHTS", "2", "3"), null);

        assertEquals(3L, ((RedisInteger) res).value());

        assertEquals(2.0, getScore("out", "A"), 0.001);   // 1*2
        assertEquals(13.0, getScore("out", "B"), 0.001);  // 2*2 + 3*3
        assertEquals(12.0, getScore("out", "C"), 0.001);  // 4*3
    }

    /**
     * 测试 4: 聚合方式 MAX (AGGREGATE MAX)
     * Set1: {A:10, B:20}
     * Set2: {A:15, B:5}
     * Union Result: {A:15(max), B:20(max)}
     */
    @Test
    void testAggregateMax() {
        createZSet("z1", 10, "A", 20, "B");
        createZSet("z2", 15, "A", 5, "B");

        // ZUNIONSTORE out 2 z1 z2 AGGREGATE MAX
        zUnion.execute(storage, args(
                "ZUNIONSTORE", "out", "2", "z1", "z2", "AGGREGATE", "MAX"), null);

        assertEquals(15.0, getScore("out", "A"));
        assertEquals(20.0, getScore("out", "B"));
    }

    /**
     * 测试 5: 交集优化逻辑验证 (Small Set Optimization)
     * 大集合: {A..Z} 26个
     * 小集合: {A} 1个
     * 交集应该只有 {A}，且遍历次数应该很少
     */
    @Test
    void testInterOptimization() {
        // Big Set
        createZSet("big", 1, "A", 2, "B", 3, "C", 4, "D", 5, "E");
        // Small Set
        createZSet("small", 10, "A");

        // ZINTERSTORE out 2 big small
        // 程序内部应该自动识别 small 为基准集合，只遍历一次 A
        RedisMessage res = zInter.execute(storage, args("ZINTERSTORE", "out", "2", "big", "small"), null);

        assertEquals(1L, ((RedisInteger) res).value());
        assertEquals(11.0, getScore("out", "A")); // 1 + 10
    }

    /**
     * 测试 6: 缺失 Key 的情况
     * Union: z1(不存在) + z2({A:1}) -> {A:1}
     * Inter: z1(不存在) ∩ z2({A:1}) -> Empty
     */
    @Test
    void testMissingKeys() {
        createZSet("z2", 1, "A");

        // Union
        RedisMessage resUnion = zUnion.execute(storage, args("ZUNIONSTORE", "u_out", "2", "missing_key", "z2"), null);
        assertEquals(1L, ((RedisInteger) resUnion).value());
        assertEquals(1.0, getScore("u_out", "A"));

        // Inter
        RedisMessage resInter = zInter.execute(storage, args("ZINTERSTORE", "i_out", "2", "missing_key", "z2"), null);
        assertEquals(0L, ((RedisInteger) resInter).value());
        assertNull(storage.get("i_out")); // 结果为空，key 不应存在
    }
}
