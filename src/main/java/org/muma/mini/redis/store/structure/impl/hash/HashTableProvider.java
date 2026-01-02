package org.muma.mini.redis.store.structure.impl.hash;

import org.muma.mini.redis.store.structure.HashProvider;
import java.util.HashMap;
import java.util.Map;

public class HashTableProvider implements HashProvider {
    private final Map<String, byte[]> table;

    public HashTableProvider() {
        this.table = new HashMap<>();
    }

    public HashTableProvider(Map<String, byte[]> initialData) {
        this.table = new HashMap<>(initialData);
    }

    @Override
    public int put(String field, byte[] value) {
        return table.put(field, value) == null ? 1 : 0;
    }

    @Override
    public byte[] get(String field) {
        return table.get(field);
    }

    @Override
    public int remove(String field) {
        return table.remove(field) != null ? 1 : 0;
    }

    @Override
    public int size() {
        return table.size();
    }

    @Override
    public Map<String, byte[]> toMap() {
        return new HashMap<>(table);
    }
}
