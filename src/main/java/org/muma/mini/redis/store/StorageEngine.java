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


    /**
     * 获取所有 Key 的迭代器 (用于 AOF Rewrite)
     * 实现必须支持并发遍历 (弱一致性)，不能抛 CME
     */
    Iterable<String> keys();


    long getDirty();

    long getLastSaveTime();

    void resetDirty(); // 保存成功后调用

}
