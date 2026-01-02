package org.muma.mini.redis.store;

import org.muma.mini.redis.common.RedisData;

public interface StorageEngine {

    // 基础 KV 操作
    RedisData get(String key);

    void put(String key, RedisData data);

    boolean remove(String key);

    // 清空数据
    void flush();

    // 原子性支持 (供 INCR 等命令使用)
    // 简单实现：提供一个对象锁，或者具体的原子操作方法
    Object getLock(String key);
}
