package org.muma.mini.redis.store.structure.impl.zset;

import org.muma.mini.redis.store.structure.ZSetProvider;

import java.util.concurrent.ThreadLocalRandom;

/**
 * ZSkipList
 * 对应 Redis zskiplist 实现，支持 O(logN) 的插入、删除、查找和 Rank 计算。
 */
public class ZSkipList {

    private static final int ZSKIPLIST_MAXLEVEL = 32;
    private static final double ZSKIPLIST_P = 0.25;

    private final ZSkipListNode header;
    private ZSkipListNode tail;
    private int length;
    private int level;

    public ZSkipList() {
        this.level = 1;
        this.length = 0;
        this.header = new ZSkipListNode(ZSKIPLIST_MAXLEVEL, 0, null);
    }

    // --- 核心方法 1: 插入节点 ---
    public ZSkipListNode insert(double score, String member) {
        ZSkipListNode[] update = new ZSkipListNode[ZSKIPLIST_MAXLEVEL]; // 记录每一层的前驱节点
        int[] rank = new int[ZSKIPLIST_MAXLEVEL]; // 记录每一层前驱节点的排名

        ZSkipListNode x = this.header;
        // 1. 从最高层向下寻找插入位置
        for (int i = this.level - 1; i >= 0; i--) {
            // 存储当前层到达 x 的 rank
            rank[i] = (i == this.level - 1) ? 0 : rank[i + 1];

            // 如果下一个节点存在，且 (score < next.score) 或者 (score == next.score && member < next.member)
            while (x.level[i].forward != null &&
                    (x.level[i].forward.score < score ||
                            (x.level[i].forward.score == score && x.level[i].forward.member.compareTo(member) < 0))) {

                rank[i] += x.level[i].span; // 累加 span 计算 rank
                x = x.level[i].forward;
            }
            update[i] = x;
        }

        // 2. 生成新节点的层数
        int lvl = randomLevel();
        if (lvl > this.level) {
            // 如果新层数高于当前最大层数，需要初始化超出的部分
            for (int i = this.level; i < lvl; i++) {
                rank[i] = 0;
                update[i] = this.header;
                update[i].level[i].span = this.length; // 初始 span 为整个链表长度
            }
            this.level = lvl;
        }

        // 3. 创建新节点
        x = new ZSkipListNode(lvl, score, member);

        // 4. 调整指针并更新 span
        for (int i = 0; i < lvl; i++) {
            // 插入链表: update[i] -> x -> update[i].forward
            x.level[i].forward = update[i].level[i].forward;
            update[i].level[i].forward = x;

            // 计算 x.span
            // x.span = update[i].span - (rank[0] - rank[i])
            x.level[i].span = update[i].level[i].span - (rank[0] - rank[i]);

            // 更新前驱节点的 span
            update[i].level[i].span = (rank[0] - rank[i]) + 1;
        }

        // 5. 对于没有触达的高层，span 需要 +1 (因为插入了一个新节点)
        for (int i = lvl; i < this.level; i++) {
            update[i].level[i].span++;
        }

        // 6. 设置后退指针 (第0层)
        x.backward = (update[0] == this.header) ? null : update[0];
        if (x.level[0].forward != null) {
            x.level[0].forward.backward = x;
        } else {
            this.tail = x; // 如果是最后一个节点，更新 tail
        }

        this.length++;
        return x;
    }

    // --- 核心方法 2: 删除节点 ---
    // 返回 1 表示删除成功，0 表示未找到
    public int delete(double score, String member) {
        ZSkipListNode[] update = new ZSkipListNode[ZSKIPLIST_MAXLEVEL];
        ZSkipListNode x = this.header;

        // 1. 寻找待删除节点的前驱
        for (int i = this.level - 1; i >= 0; i--) {
            while (x.level[i].forward != null &&
                    (x.level[i].forward.score < score ||
                            (x.level[i].forward.score == score && x.level[i].forward.member.compareTo(member) < 0))) {
                x = x.level[i].forward;
            }
            update[i] = x;
        }

        // 2. 检查是否找到
        x = x.level[0].forward;
        if (x != null && score == x.score && x.member.equals(member)) {
            deleteNode(x, update);
            return 1;
        }
        return 0; // 未找到
    }

    // 内部删除逻辑：调整指针和 span
    private void deleteNode(ZSkipListNode x, ZSkipListNode[] update) {
        for (int i = 0; i < this.level; i++) {
            if (update[i].level[i].forward == x) {
                // 跨过 x，并合并 span
                update[i].level[i].span += x.level[i].span - 1;
                update[i].level[i].forward = x.level[i].forward;
            } else {
                // 如果这一层没有指向 x (x 层数较低)，span 减 1
                update[i].level[i].span -= 1;
            }
        }

        // 处理 backward 指针
        if (x.level[0].forward != null) {
            x.level[0].forward.backward = x.backward;
        } else {
            this.tail = x.backward;
        }

        // 如果高层变空了，降低 level
        while (this.level > 1 && this.header.level[this.level - 1].forward == null) {
            this.level--;
        }
        this.length--;
    }

    // --- 核心方法 3: 更新 Score (ZINCRBY 用) ---
    public ZSkipListNode updateScore(double curScore, String member, double newScore) {
        // 更新 Score 必须先删除再插入，因为 Score 变化会影响节点在链表中的位置
        int deleted = delete(curScore, member);
        if (deleted == 0) {
            throw new IllegalArgumentException("Node not found for update");
        }
        return insert(newScore, member);
    }

    // 获取指定排名的节点 (1-based rank)
    // O(logN)
    public ZSkipListNode getNodeByRank(long rank) {
        ZSkipListNode x = this.header;
        long traversed = 0;

        for (int i = this.level - 1; i >= 0; i--) {
            while (x.level[i].forward != null && (traversed + x.level[i].span) <= rank) {
                traversed += x.level[i].span;
                x = x.level[i].forward;
            }
            if (traversed == rank) {
                return x;
            }
        }
        return null;
    }

    // --- 核心方法 4: 获取排名 (Rank) ---
    // 返回 1-based 排名，未找到返回 0
    public long getRank(double score, String member) {
        long rank = 0;
        ZSkipListNode x = this.header;

        for (int i = this.level - 1; i >= 0; i--) {
            while (x.level[i].forward != null &&
                    (x.level[i].forward.score < score ||
                            (x.level[i].forward.score == score && x.level[i].forward.member.compareTo(member) <= 0))) {

                rank += x.level[i].span;
                x = x.level[i].forward;
                if (x.member.equals(member)) {
                    return rank;
                }
            }
        }
        return 0; // Not found
    }

    // --- 辅助方法 ---
    private int randomLevel() {
        int lvl = 1;
        while ((ThreadLocalRandom.current().nextInt() & 0xFFFF) < (ZSKIPLIST_P * 0xFFFF)) {
            lvl += 1;
        }
        return Math.min(lvl, ZSKIPLIST_MAXLEVEL);
    }

    public int length() {
        return length;
    }

    // 供测试用的简单打印
    public void printDebug() {
        System.out.println("SkipList Level: " + level + ", Length: " + length);
        ZSkipListNode node = header.level[0].forward;
        while (node != null) {
            System.out.print(node + " -> ");
            node = node.level[0].forward;
        }
        System.out.println("null");
    }
}
