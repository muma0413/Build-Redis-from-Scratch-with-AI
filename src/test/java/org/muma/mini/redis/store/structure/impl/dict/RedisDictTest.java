package org.muma.mini.redis.store.structure.impl.dict;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class RedisDictTest {

    private RedisDict<String, String> dict;

    @BeforeEach
    void setUp() {
        dict = new RedisDict<>();
    }

    @Test
    void testBasicCrud() {
        assertNull(dict.put("k1", "v1"));
        assertEquals("v1", dict.get("k1"));
        assertEquals(1, dict.size());

        assertEquals("v1", dict.put("k1", "v2")); // 更新
        assertEquals("v2", dict.get("k1"));

        assertEquals("v2", dict.remove("k1"));
        assertNull(dict.get("k1"));
        assertEquals(0, dict.size());
    }

    /**
     * 核心测试：验证渐进式 Rehash 过程
     * 初始容量 4，LoadFactor 1.0。
     * 插入第 5 个元素时，应该触发 Rehash (容量变 8)。
     */
    @Test
    void testProgressiveRehash() throws Exception {
        // 1. 填满初始容量 (4)
        for (int i = 0; i < 4; i++) {
            dict.put("key-" + i, "val-" + i);
        }
        assertFalse(isRehashing(dict), "Should not rehash yet at size 4");

        // 2. 插入第 5 个 -> 触发 Rehash
        dict.put("key-4", "val-4");
        assertTrue(isRehashing(dict), "Should start rehashing at size 5");

        // 验证 ht1 已经分配
        Object[] ht1 = getHt1(dict);
        assertNotNull(ht1);
        assertEquals(8, ht1.length, "New table size should be doubled (8)");

        // 3. 执行多次操作，推动 Rehash 进度
        // 每次操作都会迁移一个 bucket。
        // 原来 ht0 大小为 4，所以理论上操作 4 次左右应该能完成迁移。

        // 操作 1: 读
        dict.get("key-0");
        assertTrue(isRehashing(dict));

        // 操作 2: 写
        dict.put("key-5", "val-5");
        assertTrue(isRehashing(dict));

        // 操作 3: 删
        dict.remove("key-1");
        assertTrue(isRehashing(dict));

        // 操作 4: 读一个不存在的
        dict.get("not-exist");

        // 此时，如果所有 bucket 都被访问到了，rehash 应该结束。
        // 但由于 hash 冲突，有些 bucket 可能为空，rehashStep 会跳过空 bucket，加速完成。
        // 为了确保完成，我们可以多做几次 dummy get
        for (int i = 0; i < 10; i++) dict.get("dummy");

        // 4. 验证 Rehash 结束
        assertFalse(isRehashing(dict), "Rehash should be finished");
        assertNull(getHt1(dict), "ht1 should be null after rehash");

        // 验证数据完整性
        assertEquals("val-0", dict.get("key-0"));
        assertEquals("val-4", dict.get("key-4"));
        assertEquals("val-5", dict.get("key-5")); // 新增的
        assertNull(dict.get("key-1")); // 刚才删掉的
    }

    @Test
    void testKeysAndValuesDuringRehash() {
        // 制造 Rehash 状态
        for (int i = 0; i < 5; i++) dict.put(String.valueOf(i), String.valueOf(i));

        // 此时正在 Rehash，部分数据在 ht0，部分在 ht1
        List<String> keys = dict.keys();
        assertEquals(5, keys.size());
        assertTrue(keys.contains("0"));
        assertTrue(keys.contains("4"));

        // 确保 keys() 遍历不会重复/遗漏 (在 rehash 期间这很难完全保证，但基础实现应该覆盖所有)
        // 我们的实现简单地遍历了 ht0 和 ht1，如果一个 key 刚被搬到 ht1，而 ht0 对应 bucket 置空了，那是安全的。
        // 风险在于：如果正在搬运同一个 bucket... 但我们的 rehashStep 是原子性的单线程操作，所以没问题。
    }

    // --- 反射辅助方法 ---

    private boolean isRehashing(RedisDict<?, ?> dict) throws Exception {
        Field field = RedisDict.class.getDeclaredField("rehashIdx");
        field.setAccessible(true);
        int idx = (int) field.get(dict);
        return idx != -1;
    }

    private Object[] getHt1(RedisDict<?, ?> dict) throws Exception {
        Field field = RedisDict.class.getDeclaredField("ht1");
        field.setAccessible(true);
        return (Object[]) field.get(dict);
    }
}
