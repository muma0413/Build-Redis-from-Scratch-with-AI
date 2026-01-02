package org.muma.mini.redis.store.structure.impl.hash;

import org.muma.mini.redis.store.structure.HashProvider;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ZipListProvider implements HashProvider {
    // k, v, k, v 交替存储
    private final List<Object> zipList = new ArrayList<>();

    @Override
    public int put(String field, byte[] value) {
        for (int i = 0; i < zipList.size(); i += 2) {
            if (zipList.get(i).equals(field)) {
                zipList.set(i + 1, value); // Update
                return 0;
            }
        }
        zipList.add(field);
        zipList.add(value); // Insert
        return 1;
    }

    @Override
    public byte[] get(String field) {
        for (int i = 0; i < zipList.size(); i += 2) {
            if (zipList.get(i).equals(field)) {
                return (byte[]) zipList.get(i + 1);
            }
        }
        return null;
    }

    @Override
    public int remove(String field) {
        for (int i = 0; i < zipList.size(); i += 2) {
            if (zipList.get(i).equals(field)) {
                zipList.remove(i + 1);
                zipList.remove(i);
                return 1;
            }
        }
        return 0;
    }

    @Override
    public int size() {
        return zipList.size() / 2;
    }

    @Override
    public Map<String, byte[]> toMap() {
        Map<String, byte[]> map = new HashMap<>();
        for (int i = 0; i < zipList.size(); i += 2) {
            map.put((String) zipList.get(i), (byte[]) zipList.get(i + 1));
        }
        return map;
    }
}
