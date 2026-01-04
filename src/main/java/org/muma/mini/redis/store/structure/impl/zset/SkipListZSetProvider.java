package org.muma.mini.redis.store.structure.impl.zset;

import org.muma.mini.redis.common.RedisZSet;
import org.muma.mini.redis.store.structure.ZSetProvider;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SkipListZSetProvider implements ZSetProvider {

    // O(1) 查找 Score
    private final Map<String, Double> dict = new HashMap<>();

    // O(logN) 排序与范围查找
    private final ZSkipList zsl = new ZSkipList();

    @Override
    public int add(double score, String member) {
        Double currentScore = dict.get(member);
        if (currentScore != null) {
            // 已存在
            if (currentScore != score) {
                // 分数变动：SkipList 需要先删后加以调整位置
                zsl.updateScore(currentScore, member, score);
                dict.put(member, score);
            }
            return 0;
        } else {
            // 新增
            zsl.insert(score, member);
            dict.put(member, score);
            return 1;
        }
    }

    @Override
    public int remove(String member) {
        Double score = dict.remove(member);
        if (score == null) {
            return 0;
        }
        // 从 SkipList 中同步删除
        return zsl.delete(score, member);
    }

    @Override
    public Double getScore(String member) {
        return dict.get(member);
    }

    @Override
    public Long getRank(String member) {
        Double score = dict.get(member);
        if (score == null) return null;

        long rank = zsl.getRank(score, member);
        // ZSkipList 返回的是 1-based rank，这里转为 0-based
        return rank == 0 ? null : rank - 1;
    }

    @Override
    public List<RedisZSet.ZSetEntry> range(long start, long stop) {
        long size = zsl.length();

        // 处理负数索引
        if (start < 0) start = size + start;
        if (stop < 0) stop = size + stop;

        // 边界修正
        if (start < 0) start = 0;
        if (start > stop || start >= size) return Collections.emptyList();
        if (stop >= size) stop = size - 1;

        // 获取起始节点 (SkipList 是 1-based，所以 +1)
        ZSkipListNode node = zsl.getNodeByRank(start + 1);

        List<RedisZSet.ZSetEntry> result = new ArrayList<>();
        long count = stop - start + 1;

        // 遍历 Level 0 的链表
        while (count > 0 && node != null) {
            result.add(new RedisZSet.ZSetEntry(node.member, node.score));
            node = node.level[0].forward;
            count--;
        }
        return result;
    }

    @Override
    public int size() {
        return dict.size();
    }

    @Override
    public List<RedisZSet.ZSetEntry> getAll() {
        return range(0, size() - 1);
    }

    @Override
    public List<RedisZSet.ZSetEntry> revRange(long start, long stop) {
        long size = zsl.length();
        // 1. 数学转换：反向排名转正向排名
        // 比如 size=10, revRange 0(第一名) 2(第三名)
        // 对应正向 rank: 9(第一名), 7(第三名) -> range(7, 9)
        long realStart = size - 1 - stop;
        long realStop = size - 1 - start;

        // 2. 复用 range 获取正序结果
        // range 方法内部会自动处理边界检查
        List<RedisZSet.ZSetEntry> list = range(realStart, realStop);

        // 3. 内存反转
        // 由于是从 range 获取的子集，数据量受 limit 限制，反转开销很小
        Collections.reverse(list);
        return list;
    }

    @Override
    public List<RedisZSet.ZSetEntry> rangeByScore(RangeSpec range, int offset, int count) {
        // 1. O(logN) 快速定位起点
        ZSkipListNode node = zsl.firstInRange(range);

        List<RedisZSet.ZSetEntry> result = new ArrayList<>();
        if (node == null) return result;

        // 2. 处理 offset
        while (offset > 0 && node != null) {
            // 这里还可以优化：如果 offset 很大，可以用 span 跳跃，
            // 但 Redis 源码对于 offset 也是简单遍历，除非做专门优化。
            // 简单遍历 Level 0 即可
            node = node.level[0].forward;
            // 检查是否还在范围内 (offset 过程中可能跳出 max)
            if (node == null || !range.contains(node.score)) return result;
            offset--;
        }

        // 3. 收集结果
        while (count > 0 && node != null) {
            if (!range.contains(node.score)) break; // 超出 max 范围，结束

            result.add(new RedisZSet.ZSetEntry(node.member, node.score));
            node = node.level[0].forward;
            count--;
        }
        return result;
    }

    @Override
    public long count(RangeSpec range) {
        // 核心优化：利用 Rank 做减法，完全避免遍历！
        // Time: O(logN)

        // 1. 找范围内第一个节点
        ZSkipListNode first = zsl.firstInRange(range);
        if (first == null) return 0;

        // 2. 找范围内最后一个节点
        ZSkipListNode last = zsl.lastInRange(range);
        if (last == null) return 0; // 理论上不应该发生

        // 3. 计算 Rank 差值
        long rankFirst = zsl.getRank(first.score, first.member);
        long rankLast = zsl.getRank(last.score, last.member);

        return rankLast - rankFirst + 1;
    }

    @Override
    public int removeRange(long start, long stop) {
        // 1. 复用 range 获取要删除的 Entry 列表
        List<RedisZSet.ZSetEntry> toRemove = range(start, stop);

        // 2. 循环删除
        int count = 0;
        for (RedisZSet.ZSetEntry entry : toRemove) {
            if (remove(entry.member()) > 0) {
                count++;
            }
        }
        return count;
    }

    @Override
    public int removeRangeByScore(RangeSpec range) {
        // 1. 复用 rangeByScore 获取列表
        // 注意：如果数据量极大，全量获取可能会 OOM。
        // 生产级优化应该分批获取删除，或者下沉到底层做指针操作。
        // Mini-Redis 暂时全量获取。
        List<RedisZSet.ZSetEntry> toRemove = rangeByScore(range, 0, Integer.MAX_VALUE);

        // 2. 循环删除
        int count = 0;
        for (RedisZSet.ZSetEntry entry : toRemove) {
            if (remove(entry.member()) > 0) {
                count++;
            }
        }
        return count;
    }

}
