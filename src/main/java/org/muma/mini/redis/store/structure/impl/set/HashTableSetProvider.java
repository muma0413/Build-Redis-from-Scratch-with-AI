package org.muma.mini.redis.store.structure.impl.set;

import org.muma.mini.redis.config.MiniRedisConfig;
import org.muma.mini.redis.store.structure.SetProvider;
import org.muma.mini.redis.store.structure.impl.dict.Dict;
import org.muma.mini.redis.store.structure.impl.dict.JdkDict;
import org.muma.mini.redis.store.structure.impl.dict.RedisDict;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class HashTableSetProvider implements SetProvider {

    // 占位符对象，模拟 Set 的行为 (value 不重要)
    private static final Object PRESENT = new Object();

    // 核心存储：使用抽象的 Dict 接口，而不是具体的 Set
    private final Dict<ByteBuffer, Object> dict;

    public HashTableSetProvider() {
        // 【核心修改】工厂模式：根据配置决定底层实现
        MiniRedisConfig.DictBackend backend = MiniRedisConfig.getInstance().getSetDictBackend();
        if (backend == MiniRedisConfig.DictBackend.JDK_HASHMAP) {
            this.dict = new JdkDict<>();
        } else {
            this.dict = new RedisDict<>();
        }
    }

    @Override
    public int add(byte[] member) {
        // Dict.put 返回旧值，如果返回 null 表示 key 之前不存在 (新增)
        return dict.put(ByteBuffer.wrap(member), PRESENT) == null ? 1 : 0;
    }

    @Override
    public int remove(byte[] member) {
        // Dict.remove 返回被删除的值，如果非 null 表示删除成功
        return dict.remove(ByteBuffer.wrap(member)) != null ? 1 : 0;
    }

    @Override
    public boolean contains(byte[] member) {
        return dict.containsKey(ByteBuffer.wrap(member));
    }

    @Override
    public int size() {
        return dict.size();
    }

    @Override
    public List<byte[]> getAll() {
        // Dict.keys() 返回 List<Key>
        List<ByteBuffer> keys = dict.keys();
        List<byte[]> result = new ArrayList<>(keys.size());
        for (ByteBuffer bb : keys) {
            result.add(toBytes(bb));
        }
        return result;
    }

    @Override
    public byte[] pop() {
        List<ByteBuffer> keys = dict.keys();
        if (keys.isEmpty()) return null;

        // 随机移除一个
        int idx = ThreadLocalRandom.current().nextInt(keys.size());
        ByteBuffer key = keys.get(idx);

        // 注意：这里 keys 是快照，并发下可能 key 已经被删了，所以 remove 可能返回 null
        // 但在 Mini-Redis 单线程模型下是安全的。
        dict.remove(key);

        return toBytes(key);
    }

    @Override
    public List<byte[]> randomMembers(int count) {
        List<ByteBuffer> keys = dict.keys();
        if (keys.isEmpty()) return Collections.emptyList();

        List<byte[]> result = new ArrayList<>();
        int actualCount = Math.min(count, keys.size());

        // 简单随机抽样 (这里为了简单可能重复，或者使用 Shuffle)
        // 为了模拟 SRANDMEMBER (count > 0 不重复)，我们应该 shuffle 或者 swap
        // 这里简化：随机取，不保证绝对随机性
        for (int i = 0; i < actualCount; i++) {
            // 优化：对于 count 接近 size 的情况，可以直接返回全部
            // 这里简单随机
            int idx = ThreadLocalRandom.current().nextInt(keys.size());
            result.add(toBytes(keys.get(idx)));
        }
        return result;
    }

    private byte[] toBytes(ByteBuffer bb) {
        byte[] bytes = new byte[bb.remaining()];
        // duplicate 避免修改原 ByteBuffer 的 position，保证在 Dict 中依然有效
        bb.duplicate().get(bytes);
        return bytes;
    }
}
