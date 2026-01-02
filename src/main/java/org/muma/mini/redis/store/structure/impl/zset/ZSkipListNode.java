package org.muma.mini.redis.store.structure.impl.zset;

/**
 * ZSkipListNode
 * 对应 Redis zskiplistNode
 */
public class ZSkipListNode {

    public final String member;
    public double score;

    public ZSkipListNode backward;
    public final ZSkipListLevel[] level;

    public ZSkipListNode(int level, double score, String member) {
        this.score = score;
        this.member = member;
        this.level = new ZSkipListLevel[level];
        for (int i = 0; i < level; i++) {
            this.level[i] = new ZSkipListLevel();
        }
    }

    public static class ZSkipListLevel {
        public ZSkipListNode forward;
        public int span; // 跨度：当前节点到 forward 节点跳过了多少个节点
    }

    @Override
    public String toString() {
        return "Node{score=" + score + ", member='" + member + "'}";
    }
}
