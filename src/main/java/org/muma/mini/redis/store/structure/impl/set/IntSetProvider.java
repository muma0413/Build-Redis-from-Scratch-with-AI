package org.muma.mini.redis.store.structure.impl.set;

import org.muma.mini.redis.store.structure.SetProvider;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * 基于有序整数数组实现的 Set 存储引擎 (IntSet)
 * <p>
 * 适用场景：元素全为整数且数量较少时。
 * 优势：内存极其紧凑，CPU 缓存友好。
 * 劣势：插入/删除需要移动数组 (O(N))。
 */
public class IntSetProvider implements SetProvider {

    // 有序存储 long 值
    // 在真实 Redis 中是 int8/int16/int32/int64 的连续内存块
    // 这里用 ArrayList<Long> 模拟
    private final List<Long> integers = new ArrayList<>();

    @Override
    public int add(byte[] member) {
        long val = parseLong(member);

        // 二分查找
        int index = Collections.binarySearch(integers, val);
        if (index >= 0) return 0; // 已存在

        // 插入点: -(insertion point) - 1
        int insertPoint = -(index + 1);
        integers.add(insertPoint, val);
        return 1;
    }

    @Override
    public int remove(byte[] member) {
        try {
            long val = parseLong(member);
            int index = Collections.binarySearch(integers, val);
            if (index >= 0) {
                integers.remove(index);
                return 1;
            }
        } catch (NumberFormatException ignored) {
            // 如果传进来的不是数字，那肯定不在 IntSet 里
        }
        return 0;
    }

    @Override
    public boolean contains(byte[] member) {
        try {
            long val = parseLong(member);
            return Collections.binarySearch(integers, val) >= 0;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    @Override
    public int size() {
        return integers.size();
    }

    @Override
    public List<byte[]> getAll() {
        List<byte[]> result = new ArrayList<>(integers.size());
        for (Long val : integers) {
            result.add(String.valueOf(val).getBytes(StandardCharsets.UTF_8));
        }
        return result;
    }

    @Override
    public byte[] pop() {
        if (integers.isEmpty()) return null;
        // 随机索引
        int idx = ThreadLocalRandom.current().nextInt(integers.size());
        Long val = integers.remove(idx);
        return String.valueOf(val).getBytes(StandardCharsets.UTF_8);
    }

    /**
     * 随机获取 N 个不重复元素
     */
    @Override
    public List<byte[]> randomMembers(int count) {
        if (integers.isEmpty()) return Collections.emptyList();

        // 1. 如果请求数 >= 总数，返回全部
        if (count >= integers.size()) {
            return getAll();
        }

        // 2. 随机抽样
        // 为了保证不重复，我们使用索引 Shuffle 算法
        List<byte[]> result = new ArrayList<>(count);
        List<Integer> indices = new ArrayList<>(integers.size());
        for (int i = 0; i < integers.size(); i++) indices.add(i);

        Collections.shuffle(indices); // O(N)

        for (int i = 0; i < count; i++) {
            Long val = integers.get(indices.get(i));
            result.add(String.valueOf(val).getBytes(StandardCharsets.UTF_8));
        }
        return result;
    }

    private long parseLong(byte[] member) {
        return Long.parseLong(new String(member, StandardCharsets.UTF_8));
    }
}
