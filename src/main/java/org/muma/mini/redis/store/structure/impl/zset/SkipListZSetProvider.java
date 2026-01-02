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
}
