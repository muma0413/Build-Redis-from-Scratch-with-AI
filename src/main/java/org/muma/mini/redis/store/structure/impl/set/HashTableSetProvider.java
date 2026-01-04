package org.muma.mini.redis.store.structure.impl.set;

import org.muma.mini.redis.store.structure.SetProvider;

import java.nio.ByteBuffer;
import java.util.*;

public class HashTableSetProvider implements SetProvider {

    // 使用 ByteBuffer 作为 Key，因为 byte[] 的 hashCode 是地址，不能去重
    private final Set<ByteBuffer> set = new HashSet<>();

    @Override
    public int add(byte[] member) {
        return set.add(ByteBuffer.wrap(member)) ? 1 : 0;
    }

    @Override
    public int remove(byte[] member) {
        return set.remove(ByteBuffer.wrap(member)) ? 1 : 0;
    }

    @Override
    public boolean contains(byte[] member) {
        return set.contains(ByteBuffer.wrap(member));
    }

    @Override
    public int size() {
        return set.size();
    }

    @Override
    public List<byte[]> getAll() {
        List<byte[]> result = new ArrayList<>(set.size());
        for (ByteBuffer bb : set) {
            // duplicate 避免修改 position
            byte[] bytes = new byte[bb.remaining()];
            bb.duplicate().get(bytes);
            result.add(bytes);
        }
        return result;
    }

    @Override
    public byte[] pop() {
        if (set.isEmpty()) return null;
        // HashSet 无法高效随机删除，必须遍历 Iterator
        Iterator<ByteBuffer> it = set.iterator();
        ByteBuffer bb = it.next();
        it.remove();
        return toBytes(bb);
    }

    @Override
    public List<byte[]> randomMembers(int count) {
        if (set.isEmpty()) return Collections.emptyList();
        List<byte[]> result = new ArrayList<>();
        Iterator<ByteBuffer> it = set.iterator();
        while (it.hasNext() && result.size() < count) {
            result.add(toBytes(it.next()));
        }
        return result;
    }

    private byte[] toBytes(ByteBuffer bb) {
        byte[] bytes = new byte[bb.remaining()];
        bb.duplicate().get(bytes);
        return bytes;
    }
}
