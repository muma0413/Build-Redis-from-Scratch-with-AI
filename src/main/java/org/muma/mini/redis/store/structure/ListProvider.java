package org.muma.mini.redis.store.structure;

import java.util.List;

public interface ListProvider {
    void lpush(byte[] element); // 头部插入

    void rpush(byte[] element); // 尾部插入

    byte[] lpop();              // 头部弹出

    byte[] rpop();              // 尾部弹出

    int size();

    // 范围查询 (支持负数索引)
    List<byte[]> range(long start, long stop);

    // 获取指定索引元素
    byte[] index(long index);

    // 设置指定索引元素
    void set(long index, byte[] element);

    // 在 pivot 前/后 插入 value
    // 返回 list 长度，-1 表示 pivot 未找到
    int insert(boolean before, byte[] pivot, byte[] value);

    // 移除元素
    // count > 0: 从头往尾移除 count 个
    // count < 0: 从尾往头移除 |count| 个
    // count = 0: 移除所有
    int remove(long count, byte[] element);

    // 修剪列表，只保留 [start, stop] 区间
    void trim(long start, long stop);
}
