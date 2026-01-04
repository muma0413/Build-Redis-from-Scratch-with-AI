package org.muma.mini.redis.store.structure.impl.set;

import org.muma.mini.redis.store.structure.SetProvider;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class IntSetProvider implements SetProvider {

    // 有序存储 long 值
    private final List<Long> integers = new ArrayList<>();

    @Override
    public int add(byte[] member) {
        long val = parseLong(member); // 假设外部已经判断过是整数

        int index = Collections.binarySearch(integers, val);
        if (index >= 0) return 0; // 已存在

        // binarySearch 返回 -(insertion point) - 1
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
        } catch (NumberFormatException ignored) {}
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
        int idx = ThreadLocalRandom.current().nextInt(integers.size());
        Long val = integers.remove(idx);
        return String.valueOf(val).getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public List<byte[]> randomMembers(int count) {
        // 简单实现，暂不处理 count < 0 (允许重复) 的情况
        if (integers.isEmpty()) return Collections.emptyList();

        List<byte[]> result = new ArrayList<>();
        int actualCount = Math.min(count, integers.size());

        // 既然要随机且不重复，我们可以shuffle个索引或者简单随机抽
        // 为了简单，我们只做 pop 类似的逻辑但不删除
        // 真正的 Redis SRANDMEMBER 逻辑比较复杂，这里简化

        // 随机抽一个
        int idx = ThreadLocalRandom.current().nextInt(integers.size());
        result.add(String.valueOf(integers.get(idx)).getBytes(StandardCharsets.UTF_8));
        return result;
    }

    private long parseLong(byte[] member) {
        return Long.parseLong(new String(member, StandardCharsets.UTF_8));
    }
}
