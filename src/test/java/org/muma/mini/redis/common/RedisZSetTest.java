package org.muma.mini.redis.common;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class RedisZSetTest {

    private RedisZSet zset;

    @BeforeEach
    void setUp() {
        zset = new RedisZSet();
    }

    @Test
    void testAddAndScore() {
        // 1. 新增
        assertEquals(1, zset.add(10.0, "Alice"));
        assertEquals(1, zset.add(20.0, "Bob"));

        // 验证分数
        assertEquals(10.0, zset.getScore("Alice"));
        assertEquals(20.0, zset.getScore("Bob"));
        assertNull(zset.getScore("Charlie"));

        // 2. 更新 (Update)
        assertEquals(0, zset.add(30.0, "Alice")); // 分数变了
        assertEquals(30.0, zset.getScore("Alice"));

        // 验证 Map 和 SkipList 大小一致
        assertEquals(2, zset.size());
    }

    @Test
    void testRank() {
        // 构造数据：Alice(10), Bob(20), Charlie(30)
        zset.add(20.0, "Bob");
        zset.add(10.0, "Alice");
        zset.add(30.0, "Charlie");

        // 验证排名 (ZRANK 是 0-based)
        // Alice(10) -> 0
        // Bob(20)   -> 1
        // Charlie(30)-> 2
        assertEquals(0L, zset.getRank("Alice"));
        assertEquals(1L, zset.getRank("Bob"));
        assertEquals(2L, zset.getRank("Charlie"));

        // 验证不存在的成员
        assertNull(zset.getRank("David"));
    }

    @Test
    void testSameScoreRank() {
        // 验证分数相同时，按字典序排列
        // "A"(10), "B"(10) -> "A" < "B"
        zset.add(10.0, "B");
        zset.add(10.0, "A");

        assertEquals(0L, zset.getRank("A"));
        assertEquals(1L, zset.getRank("B"));
    }

    @Test
    void testRemove() {
        zset.add(10.0, "A");
        zset.add(20.0, "B");
        zset.add(30.0, "C");

        // 删除中间的 B
        assertEquals(1, zset.remove("B"));
        assertNull(zset.getScore("B"));
        assertEquals(2, zset.size());

        // 验证排名变化：C 的排名应该前移 1 位
        // A(0), C(1)
        assertEquals(0L, zset.getRank("A"));
        assertEquals(1L, zset.getRank("C"));

        // 删除不存在的
        assertEquals(0, zset.remove("X"));
    }

    @Test
    void testIncrBy() {
        // 新增自增
        assertEquals(5.0, zset.incrBy(5.0, "A"));

        // 现有自增
        assertEquals(8.0, zset.incrBy(3.0, "A"));

        // 负数自增 (减)
        assertEquals(6.0, zset.incrBy(-2.0, "A"));

        assertEquals(6.0, zset.getScore("A"));
    }

    @Test
    void testMassiveInsertion() {
        // 压力测试：插入 1000 个元素，验证 Rank 是否正确 (检测 span 维护逻辑)
        int count = 1000;
        for (int i = 0; i < count; i++) {
            zset.add(i, "user:" + i);
        }

        assertEquals(count, zset.size());

        // 随机抽查排名
        // Score=0 -> Rank=0
        // Score=500 -> Rank=500
        // Score=999 -> Rank=999
        assertEquals(0L, zset.getRank("user:0"));
        assertEquals(500L, zset.getRank("user:500"));
        assertEquals(999L, zset.getRank("user:999"));
    }
}
