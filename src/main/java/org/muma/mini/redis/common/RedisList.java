package org.muma.mini.redis.common;

import org.muma.mini.redis.store.structure.ListProvider;
import org.muma.mini.redis.store.structure.impl.list.QuickList;

import java.io.Serializable;
import java.util.List;

/**
 * Redis List 核心封装类
 * 目前底层采用 QuickList (双向链表 + ZipList 节点) 实现。
 * <p>
 * 职责：
 * 1. 屏蔽底层 ListProvider 的具体实现。
 * 2. 提供统一的 API 给 Command 层调用。
 */
public class RedisList implements Serializable {

    // 核心存储引擎
    private final ListProvider provider;

    public RedisList() {
        // Redis 3.2+ 默认使用 QuickList 作为 List 的唯一实现
        // 之前提到的 LinkedListProvider 已经被淘汰，所以这里直接硬编码 QuickList
        this.provider = new QuickList();
    }

    /**
     * 头部插入 (LPUSH)
     * O(1)
     */
    public void lpush(byte[] element) {
        provider.lpush(element);
    }

    /**
     * 尾部插入 (RPUSH)
     * O(1)
     */
    public void rpush(byte[] element) {
        provider.rpush(element);
    }

    /**
     * 头部弹出 (LPOP)
     * O(1)
     */
    public byte[] lpop() {
        return provider.lpop();
    }

    /**
     * 尾部弹出 (RPOP)
     * O(1)
     */
    public byte[] rpop() {
        return provider.rpop();
    }

    /**
     * 获取长度 (LLEN)
     * O(1)
     */
    public int size() {
        return provider.size();
    }

    /**
     * 范围查询 (LRANGE)
     * O(N)
     */
    public List<byte[]> range(long start, long stop) {
        return provider.range(start, stop);
    }

    /**
     * 获取指定索引元素 (LINDEX)
     * O(N)
     */
    public byte[] index(long index) {
        return provider.index(index);
    }

    /**
     * 设置指定索引元素 (LSET)
     * O(N)
     */
    public void set(long index, byte[] element) {
        provider.set(index, element);
    }

    public int insert(boolean before, byte[] pivot, byte[] value) {
        return provider.insert(before, pivot, value);
    }

    public int remove(long count, byte[] element) {
        return provider.remove(count, element);
    }

    public void trim(long start, long stop) {
        provider.trim(start, stop);
    }

}
