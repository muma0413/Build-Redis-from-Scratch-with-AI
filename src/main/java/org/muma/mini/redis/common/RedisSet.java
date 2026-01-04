package org.muma.mini.redis.common;

import org.muma.mini.redis.store.structure.SetProvider;
import org.muma.mini.redis.store.structure.impl.set.HashTableSetProvider;
import org.muma.mini.redis.store.structure.impl.set.IntSetProvider;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class RedisSet implements Serializable {

    private static final int INTSET_MAX_ENTRIES = 512;

    private SetProvider provider;

    public RedisSet() {
        // 默认尝试 IntSet
        this.provider = new IntSetProvider();
    }

    public int add(byte[] member) {
        // 1. 检查是否需要升级 (不是整数)
        if (isIntSet() && !isInteger(member)) {
            upgrade();
        }

        int res = provider.add(member);

        // 2. 检查是否需要升级 (数量过多)
        if (isIntSet() && provider.size() > INTSET_MAX_ENTRIES) {
            upgrade();
        }
        return res;
    }

    public int remove(byte[] member) {
        return provider.remove(member);
    }

    public boolean contains(byte[] member) {
        return provider.contains(member);
    }

    public int size() {
        return provider.size();
    }

    public List<byte[]> getAll() {
        return provider.getAll();
    }

    public byte[] pop() {
        return provider.pop();
    }

    public List<byte[]> randomMembers(int count) {
        return provider.randomMembers(count);
    }

    // --- 内部逻辑 ---

    private boolean isIntSet() {
        return provider instanceof IntSetProvider;
    }

    private boolean isInteger(byte[] member) {
        try {
            Long.parseLong(new String(member, StandardCharsets.UTF_8));
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    private void upgrade() {
        // System.out.println("Upgrading Set from IntSet to HashTable...");
        List<byte[]> allData = provider.getAll();
        HashTableSetProvider newProvider = new HashTableSetProvider();
        for (byte[] item : allData) {
            newProvider.add(item);
        }
        this.provider = newProvider;
    }
}
