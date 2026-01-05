package org.muma.mini.redis.store.structure.impl.dict;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JdkDict<K, V> implements Dict<K, V> {
    private final Map<K, V> map = new HashMap<>();

    @Override
    public V get(K key) {
        return map.get(key);
    }

    @Override
    public V put(K key, V value) {
        return map.put(key, value);
    }

    @Override
    public V remove(K key) {
        return map.remove(key);
    }

    @Override
    public int size() {
        return map.size();
    }

    @Override
    public boolean containsKey(K key) {
        return map.containsKey(key);
    }

    @Override
    public List<K> keys() {
        return new ArrayList<>(map.keySet());
    }
}
