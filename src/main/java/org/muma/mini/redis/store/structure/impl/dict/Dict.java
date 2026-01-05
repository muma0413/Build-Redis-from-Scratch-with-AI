package org.muma.mini.redis.store.structure.impl.dict;

import java.util.List;

/**
 * 字典抽象接口
 * 屏蔽 JDK HashMap 和 RedisDict 的差异
 */
public interface Dict<K, V> {
    V get(K key);

    V put(K key, V value);

    V remove(K key);

    int size();

    boolean containsKey(K key);

    List<K> keys();
    // 其他需要的方法...
}
