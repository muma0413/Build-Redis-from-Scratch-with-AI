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

/**
 * 基于哈希表实现的 Set 存储引擎
 * <p>
 * 适用场景：元素数量较多或包含非整数字符串时。
 * 底层实现：依赖 {@link Dict} 接口，支持配置切换 (JDK HashMap 或 Custom RedisDict)。
 */
public class HashTableSetProvider implements SetProvider {

    // 占位符对象，模拟 Set 的行为 (Map 的 Value 不重要)
    private static final Object PRESENT = new Object();

    // 核心存储：使用抽象的 Dict 接口
    private final Dict<ByteBuffer, Object> dict;

    /**
     * 构造函数：根据全局配置初始化底层 Dict 实现
     */
    public HashTableSetProvider() {
        MiniRedisConfig.DictBackend backend = MiniRedisConfig.getInstance().getSetDictBackend();
        if (backend == MiniRedisConfig.DictBackend.JDK_HASHMAP) {
            this.dict = new JdkDict<>();
        } else {
            this.dict = new RedisDict<>();
        }
    }

    /**
     * 添加元素
     * @return 1 if new element added, 0 if element already exists
     */
    @Override
    public int add(byte[] member) {
        // Dict.put 返回旧值，如果返回 null 表示 key 之前不存在 (即新增成功)
        return dict.put(ByteBuffer.wrap(member), PRESENT) == null ? 1 : 0;
    }

    /**
     * 移除元素
     * @return 1 if removed, 0 if not exists
     */
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

    /**
     * 获取所有元素 (无序)
     */
    @Override
    public List<byte[]> getAll() {
        // Dict.keys() 返回 List<ByteBuffer>
        List<ByteBuffer> keys = dict.keys();
        List<byte[]> result = new ArrayList<>(keys.size());
        for (ByteBuffer bb : keys) {
            result.add(toBytes(bb));
        }
        return result;
    }

    /**
     * 随机弹出一个元素 (SPOP)
     * O(N) in worst case (due to keys() copy), O(1) amortized if lucky.
     */
    @Override
    public byte[] pop() {
        List<ByteBuffer> keys = dict.keys();
        if (keys.isEmpty()) return null;

        // 随机移除一个
        // 注意：在大数据量下，keys() 会产生全量拷贝，性能较差。
        // 理想实现是在 Dict 中支持 getRandomKey()。
        int idx = ThreadLocalRandom.current().nextInt(keys.size());
        ByteBuffer key = keys.get(idx);

        dict.remove(key);

        return toBytes(key);
    }

    /**
     * 随机获取 N 个不重复元素 (SRANDMEMBER count > 0)
     * O(N) due to shuffle.
     */
    @Override
    public List<byte[]> randomMembers(int count) {
        List<ByteBuffer> keys = dict.keys();
        if (keys.isEmpty()) return Collections.emptyList();

        int size = keys.size();
        // 如果请求的数量大于等于总数，直接返回全部
        if (count >= size) {
            return getAll();
        }

        // Mini-Redis 暂不支持负数 count (允许重复抽样)
        // 此处逻辑为：不重复抽样

        List<byte[]> result = new ArrayList<>(count);

        // 算法优化：使用 Shuffle 来保证随机性
        // 创建索引列表 [0, 1, ..., size-1]
        List<Integer> indices = new ArrayList<>(size);
        for (int i = 0; i < size; i++) indices.add(i);

        Collections.shuffle(indices);

        // 取前 count 个索引
        for (int i = 0; i < count; i++) {
            result.add(toBytes(keys.get(indices.get(i))));
        }

        return result;
    }

    /**
     * 辅助方法：将 ByteBuffer 转为 byte[]
     * 必须使用 duplicate() 防止修改原 Buffer 的 position
     */
    private byte[] toBytes(ByteBuffer bb) {
        byte[] bytes = new byte[bb.remaining()];
        bb.duplicate().get(bytes);
        return bytes;
    }
}
