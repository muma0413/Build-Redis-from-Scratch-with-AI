package org.muma.mini.redis.store.structure.impl.zset;

import org.muma.mini.redis.common.RedisZSet;
import org.muma.mini.redis.store.structure.ZSetProvider;

import java.util.ArrayList;
import java.util.Collections;
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

    @Override
    public List<RedisZSet.ZSetEntry> revRange(long start, long stop) {
        int size = size();
        // 1. 转换索引: 正向 Range 的索引
        // RevStart(0) -> Index(size-1)
        long realStart = size - 1 - stop;
        long realStop = size - 1 - start;

        // 2. 复用 range 方法获取正向列表
        List<RedisZSet.ZSetEntry> list = range(realStart, realStop);

        // 3. 反转列表
        Collections.reverse(list);
        return list;
    }

    @Override
    public List<RedisZSet.ZSetEntry> rangeByScore(RangeSpec range, int offset, int count) {
        List<RedisZSet.ZSetEntry> result = new ArrayList<>();
        int skipped = 0;

        // 线性遍历: member, score, member, score...
        for (int i = 0; i < list.size(); i += 2) {
            double score = (Double) list.get(i + 1);

            // ZipList 是有序的，如果超过 max 直接退出 (优化点)
            if (range.maxex ? score >= range.max : score > range.max) break;

            if (range.contains(score)) {
                if (skipped < offset) {
                    skipped++;
                    continue;
                }
                result.add(new RedisZSet.ZSetEntry((String) list.get(i), score));
                if (result.size() == count) break;
            }
        }
        return result;
    }

    @Override
    public long count(RangeSpec range) {
        long count = 0;
        for (int i = 1; i < list.size(); i += 2) {
            double score = (Double) list.get(i);
            if (range.maxex ? score >= range.max : score > range.max) break; // 提前退出
            if (range.contains(score)) {
                count++;
            }
        }
        return count;
    }

    @Override
    public int removeRange(long start, long stop) {
        int size = size();
        if (start < 0) start = size + start;
        if (stop < 0) stop = size + stop;
        if (start < 0) start = 0;
        if (start > stop || start >= size) return 0;
        if (stop >= size) stop = size - 1;

        int removed = 0;
        // 从后往前删，避免索引错位
        // ZipList 物理索引: Rank * 2
        for (long r = stop; r >= start; r--) {
            int idx = (int) (r * 2);
            list.remove(idx + 1);
            list.remove(idx);
            removed++;
        }
        return removed;
    }

    @Override
    public int removeRangeByScore(RangeSpec range) {
        int removed = 0;
        // 必须从前往后遍历，因为删除了元素索引会变，
        // 或者使用迭代器，或者倒序遍历。
        // 由于是 ArrayList，倒序遍历比较安全且高效。
        for (int i = list.size() - 2; i >= 0; i -= 2) {
            double score = (Double) list.get(i + 1);
            if (range.contains(score)) {
                list.remove(i + 1);
                list.remove(i);
                removed++;
            }
        }
        return removed;
    }

}
