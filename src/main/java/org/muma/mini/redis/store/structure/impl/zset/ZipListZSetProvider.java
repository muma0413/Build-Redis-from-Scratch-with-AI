package org.muma.mini.redis.store.structure.impl.zset;

import org.muma.mini.redis.common.RedisZSet;
import org.muma.mini.redis.store.structure.ZSetProvider;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 基于 ArrayList 模拟的 ZipList ZSet 存储引擎
 * <p>
 * 适用场景：元素数量少且 Member 短小。
 * 优势：内存极度紧凑。
 * 劣势：插入删除 O(N)。
 */
public class ZipListZSetProvider implements ZSetProvider {

    // member, score, member, score...
    private final List<Object> list = new ArrayList<>();

    @Override
    public int add(double score, String member) {
        int existingIndex = findMemberIndex(member);
        if (existingIndex != -1) {
            double oldScore = (double) list.get(existingIndex + 1);
            if (oldScore == score) {
                return 0;
            }
            list.remove(existingIndex + 1);
            list.remove(existingIndex);
        }

        // 插入排序 (O(N))
        int insertIndex = 0;
        while (insertIndex < list.size()) {
            String curMember = (String) list.get(insertIndex);
            double curScore = (double) list.get(insertIndex + 1);
            if (score < curScore || (score == curScore && member.compareTo(curMember) < 0)) {
                break;
            }
            insertIndex += 2;
        }

        list.add(insertIndex, member);
        list.add(insertIndex + 1, score);
        return existingIndex != -1 ? 0 : 1;
    }

    @Override
    public int remove(String member) {
        int idx = findMemberIndex(member);
        if (idx != -1) {
            list.remove(idx + 1);
            list.remove(idx);
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
        return (long) (idx / 2);
    }

    @Override
    public List<RedisZSet.ZSetEntry> range(long start, long stop) {
        int size = size();

        // 1. 负数归一化
        if (start < 0) start = size + start;
        if (stop < 0) stop = size + stop;

        // 2. 边界修正
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
        return range(0, size() - 1);
    }

    // --- 核心修复：ZREVRANGE ---
    @Override
    public List<RedisZSet.ZSetEntry> revRange(long start, long stop) {
        int size = size();
        if (size == 0) return Collections.emptyList();

        // 1. 先归一化索引 (处理负数)
        if (start < 0) start = size + start;
        if (stop < 0) stop = size + stop;

        // 2. 边界检查
        if (start < 0) start = 0;
        if (stop < 0) stop = 0;

        if (start > stop || start >= size) {
            return Collections.emptyList();
        }
        if (stop >= size) stop = size - 1;

        // 3. 转换为正向索引
        long realStart = size - 1 - stop;
        long realStop = size - 1 - start;

        // 4. 获取正序
        List<RedisZSet.ZSetEntry> list = range(realStart, realStop);

        // 5. 反转
        Collections.reverse(list);
        return list;
    }

    @Override
    public List<RedisZSet.ZSetEntry> rangeByScore(RangeSpec range, int offset, int count) {
        List<RedisZSet.ZSetEntry> result = new ArrayList<>();
        int skipped = 0;
        for (int i = 0; i < list.size(); i += 2) {
            double score = (Double) list.get(i + 1);
            if (range.maxex ? score >= range.max : score > range.max) break; // ZipList 有序，提前退出
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
            if (range.maxex ? score >= range.max : score > range.max) break;
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
        // 从后往前删，索引安全
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
        // 从后往前删
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

    private int findMemberIndex(String member) {
        for (int i = 0; i < list.size(); i += 2) {
            if (list.get(i).equals(member)) {
                return i;
            }
        }
        return -1;
    }
}
