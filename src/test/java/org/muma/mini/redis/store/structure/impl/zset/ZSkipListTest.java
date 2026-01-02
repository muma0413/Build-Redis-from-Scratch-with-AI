package org.muma.mini.redis.store.structure.impl.zset;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ZSkipListTest {

    private ZSkipList skipList;

    @BeforeEach
    void setUp() {
        skipList = new ZSkipList();
    }

    /**
     * 测试 1: 基础插入与排序
     * 验证跳表是否真的有序
     */
    @Test
    void testInsertOrder() {
        skipList.insert(30.0, "C");
        skipList.insert(10.0, "A");
        skipList.insert(20.0, "B");

        assertEquals(3, skipList.length());

        // 验证 rank 顺序
        assertEquals(1, skipList.getRank(10.0, "A")); // Rank 1
        assertEquals(2, skipList.getRank(20.0, "B")); // Rank 2
        assertEquals(3, skipList.getRank(30.0, "C")); // Rank 3
    }

    /**
     * 测试 2: 相同 Score 的字典序排序
     * Redis 规则：Score 相同，Member 字典序小的在前
     */
    @Test
    void testSameScoreOrder() {
        skipList.insert(100.0, "Bob");
        skipList.insert(100.0, "Alice");
        skipList.insert(100.0, "Cindy");

        // 期望顺序: Alice(1) -> Bob(2) -> Cindy(3)
        assertEquals(1, skipList.getRank(100.0, "Alice"));
        assertEquals(2, skipList.getRank(100.0, "Bob"));
        assertEquals(3, skipList.getRank(100.0, "Cindy"));
    }

    /**
     * 测试 3: 删除逻辑 (最复杂的指针调整)
     * 删除首节点、尾节点、中间节点，验证链表是否断裂
     */
    @Test
    void testDelete() {
        skipList.insert(1.0, "Head");
        skipList.insert(2.0, "Mid");
        skipList.insert(3.0, "Tail");

        // 删除中间
        assertEquals(1, skipList.delete(2.0, "Mid"));
        assertEquals(2, skipList.length());
        assertEquals(0, skipList.getRank(2.0, "Mid")); // 查不到了

        // 验证前后连接: Head -> Tail
        // Head(1) -> Tail(2)
        assertEquals(1, skipList.getRank(1.0, "Head"));
        assertEquals(2, skipList.getRank(3.0, "Tail"));

        // 删除头部
        assertEquals(1, skipList.delete(1.0, "Head"));
        assertEquals(1, skipList.getRank(3.0, "Tail")); // Tail 变成 Rank 1

        // 删除不存在的
        assertEquals(0, skipList.delete(99.0, "Ghost"));
    }

    /**
     * 测试 4: 更新 Score (updateScore)
     * 验证更新后，元素位置是否自动调整
     */
    @Test
    void testUpdateScore() {
        skipList.insert(10.0, "Player1"); // Rank 1
        skipList.insert(20.0, "Player2"); // Rank 2

        // 将 Player1 的分数从 10 改为 30
        // 期望位置变动: Player2(Rank 1) -> Player1(Rank 2)
        skipList.updateScore(10.0, "Player1", 30.0);

        assertEquals(1, skipList.getRank(20.0, "Player2"));
        assertEquals(2, skipList.getRank(30.0, "Player1"));
    }

    /**
     * 测试 5: Span (跨度) 计算的正确性
     * 这是最容易写错的地方。我们插入大量数据，然后随机抽查 Rank，
     * 如果 span 维护错了，Rank 计算一定会错。
     */
    @Test
    void testSpanConsistency() {
        int N = 2000;
        for (int i = 0; i < N; i++) {
            skipList.insert(i, "user:" + i);
        }

        assertEquals(N, skipList.length());

        // 边界测试
        assertEquals(1, skipList.getRank(0, "user:0"));
        assertEquals(N, skipList.getRank(N - 1, "user:" + (N - 1)));

        // 中间抽查
        assertEquals(1001, skipList.getRank(1000, "user:1000"));

        // 删除一半数据，再验证 Span
        for (int i = 0; i < N; i += 2) { // 删除偶数索引: 0, 2, 4...
            skipList.delete(i, "user:" + i);
        }
        // 剩下 1000 个元素 (1, 3, 5...)
        // 原来的 user:1 (score 1) 现在的 rank 应该是 1
        // 原来的 user:3 (score 3) 现在的 rank 应该是 2
        assertEquals(1000, skipList.length());
        assertEquals(1, skipList.getRank(1, "user:1"));
        assertEquals(2, skipList.getRank(3, "user:3"));
    }

    /**
     * 测试 6: 随机层数分布 (概率测试)
     * 验证 randomLevel 是否能生成高层节点
     * 多次运行，确保不会永远只有 Level 1
     */
    @RepeatedTest(5)
    void testRandomLevelDistribution() {
        ZSkipList list = new ZSkipList();
        int maxLevelSeen = 1;
        for (int i = 0; i < 1000; i++) {
            ZSkipListNode node = list.insert(i, String.valueOf(i));
            if (node.level.length > maxLevelSeen) {
                maxLevelSeen = node.level.length;
            }
        }
        // 插入 1000 个节点，根据概率 P=0.25，
        // 出现 Level 2 的概率很大，Level 4/5 也很常见。
        // 如果 maxLevel 还是 1，说明 randomLevel 算法有问题。
        assertTrue(maxLevelSeen > 1, "SkipList should generate multi-level nodes");
        System.out.println("Max Level generated: " + maxLevelSeen);
    }
}
