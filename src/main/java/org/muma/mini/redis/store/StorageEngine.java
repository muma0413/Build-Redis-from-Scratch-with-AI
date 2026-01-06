package org.muma.mini.redis.store;

import org.muma.mini.redis.common.RedisData;
import org.muma.mini.redis.protocol.RedisArray;
import org.muma.mini.redis.server.BlockingManager;

public interface StorageEngine {

    // 基础 KV 操作
    // 【修改点 1】 返回通配符类型，表示 "某种 RedisData"
    RedisData<?> get(String key);

    // 【修改点 2】 接收通配符类型，允许存入任何类型的 RedisData
    void put(String key, RedisData<?> data);

    boolean remove(String key);

    // 清空数据
    void flush();

    // 原子性支持 (供 INCR 等命令使用)
    // 简单实现：提供一个对象锁，或者具体的原子操作方法
    Object getLock(String key);

    BlockingManager getBlockingManager();

    // 用于内部组件 (如 BlockingManager) 手动传播 AOF
    void appendAof(RedisArray command);
}
