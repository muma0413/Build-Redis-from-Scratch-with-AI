package org.muma.mini.redis.store.structure.impl.zset;

import org.muma.mini.redis.common.RedisZSet;
import org.muma.mini.redis.store.structure.ZSetProvider;

import java.util.ArrayList;
import java.util.List;

public class ZipListZSetProvider implements ZSetProvider {

    // 模拟 ZipList: member, score, member, score... 且保持有序
    private final List<Object> list = new ArrayList<>();

    @Override
    public int add(double score, String member) {
        // 1. 检查是否存在
        int existingIndex = findMemberIndex(member);
        if (existingIndex != -1) {
            double oldScore = (double) list.get(existingIndex + 1);
            if (oldScore == score) {
                return 0; // 分数没变，无需操作
            }
            // 分数变了：先删除旧的，再重新寻找位置插入新的
            list.remove(existingIndex + 1); // remove score
            list.remove(existingIndex);     // remove member
        }

        // 2. 寻找插入位置 (保持 Score 升序，Score 相同按 Member 字典序)
        int insertIndex = 0;
        while (insertIndex < list.size()) {
            String curMember = (String) list.get(insertIndex);
            double curScore = (double) list.get(insertIndex + 1);

            if (score < curScore || (score == curScore && member.compareTo(curMember) < 0)) {
                break;
            }
            insertIndex += 2;
        }

        // 3. 插入
        list.add(insertIndex, member);
        list.add(insertIndex + 1, score);

        return existingIndex != -1 ? 0 : 1;
    }

    @Override
    public int remove(String member) {
        int idx = findMemberIndex(member);
        if (idx != -1) {
            list.remove(idx + 1); // remove score
            list.remove(idx);     // remove member
            return 1;
        }
        return 0;
    }

    @Override
    public Double getScore(String member) {
        int idx = findMemberIndex(member);
        return idx != -1 ? (Double) list.get(idx + 1) : null;
    }

    @Override
    public Long getRank(String member) {
        int idx = findMemberIndex(member);
        if (idx == -1) return null;
        // ZipList 索引是 0, 2, 4... 对应的 Rank 是 0, 1, 2...
        return (long) (idx / 2);
    }

    @Override
    public List<RedisZSet.ZSetEntry> range(long start, long stop) {
        int size = size();

        // 处理负数索引
        if (start < 0) start = size + start;
        if (stop < 0) stop = size + stop;

        // 边界修正
        if (start < 0) start = 0;
        if (start > stop || start >= size) return new ArrayList<>();
        if (stop >= size) stop = size - 1;

        List<RedisZSet.ZSetEntry> result = new ArrayList<>();
        for (long i = start; i <= stop; i++) {
            int idx = (int) (i * 2);
            String m = (String) list.get(idx);
            Double s = (Double) list.get(idx + 1);
            result.add(new RedisZSet.ZSetEntry(m, s));
        }
        return result;
    }

    @Override
    public int size() {
        return list.size() / 2;
    }

    @Override
    public List<RedisZSet.ZSetEntry> getAll() {
        // getAll 本质上就是获取全量 Range
        return range(0, size() - 1);
    }

    // --- 内部辅助方法 ---
    private int findMemberIndex(String member) {
        for (int i = 0; i < list.size(); i += 2) {
            if (list.get(i).equals(member)) {
                return i;
            }
        }
        return -1;
    }
}
