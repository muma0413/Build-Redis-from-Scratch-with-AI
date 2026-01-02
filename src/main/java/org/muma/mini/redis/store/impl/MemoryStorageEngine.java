package org.muma.mini.redis.store.impl;

import org.muma.mini.redis.common.RedisData;
import org.muma.mini.redis.store.StorageEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class MemoryStorageEngine implements StorageEngine {

    private static final Logger log = LoggerFactory.getLogger(MemoryStorageEngine.class);

    // 1. 数据存储 (Key -> Data)
    private final Map<String, RedisData> memoryDb = new ConcurrentHashMap<>();

    // 2. 过期时间存储 (Key -> ExpireAt Timestamp)
    // 专门维护这个 Map，可以让清理线程只关注需要过期的 Key，极大提升效率
    private final Map<String, Long> ttlMap = new ConcurrentHashMap<>();

    // 3. 定期清理线程池
    private final ScheduledExecutorService cleanupExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "Redis-Active-Cleanup");
        t.setDaemon(true); // 设置为守护线程，防止阻碍 JVM 关闭
        return t;
    });

    public MemoryStorageEngine() {
        // 启动定期清理任务：每 1000ms 执行一次
        cleanupExecutor.scheduleAtFixedRate(this::activeExpireCycle, 1, 1000, TimeUnit.MILLISECONDS);
    }

    /**
     * 定期删除策略 (简化版 Redis 算法)
     * 每次抽取一部分 Key 进行检查
     */
    private void activeExpireCycle() {
        if (ttlMap.isEmpty()) return;

        // 每次最多扫描 20 个 Key (防止卡顿)
        int sampleSize = 20;
        int expiredCount = 0;

        // ConcurrentHashMap 的迭代器是弱一致性的，且不保证随机，
        // 但对于清理任务来说，顺序扫描也是可以接受的。
        Iterator<Map.Entry<String, Long>> iterator = ttlMap.entrySet().iterator();

        long now = System.currentTimeMillis();

        int loop = 0;
        while (iterator.hasNext() && loop < sampleSize) {
            Map.Entry<String, Long> entry = iterator.next();
            String key = entry.getKey();
            Long expireAt = entry.getValue();

            if (now > expireAt) {
                // 已过期：删除数据和TTL记录
                memoryDb.remove(key);
                iterator.remove(); // 从 ttlMap 中移除
                expiredCount++;
            }
            loop++;
        }

        // 记录日志 (可选，调试用)
        if (expiredCount > 0) {
            log.debug("Active cleanup: scanned {}, expired {}", loop, expiredCount);
        }

        // 进阶优化：如果发现过期比例很高 (比如 > 25%)，其实应该立即再次触发清理，
        // 这里为了 Mini-Redis 简单，暂时只做固定频率清理。
    }

    @Override
    public RedisData get(String key) {
        RedisData data = memoryDb.get(key);
        if (data == null) return null;

        // 惰性删除 (Lazy Expiration) 依然保留作为双重保障
        if (data.isExpired()) {
            remove(key);
            return null;
        }
        return data;
    }

    @Override
    public void put(String key, RedisData data) {
        memoryDb.put(key, data);
        // 如果数据有过期时间，记录到 ttlMap；如果没有，尝试从 ttlMap 移除 (可能由有过期变为无过期)
        if (data.getExpireAt() != -1) {
            ttlMap.put(key, data.getExpireAt());
        } else {
            ttlMap.remove(key);
        }
    }

    @Override
    public boolean remove(String key) {
        ttlMap.remove(key); // 记得同步移除 TTL
        return memoryDb.remove(key) != null;
    }

    @Override
    public void flush() {
        memoryDb.clear();
        ttlMap.clear();
    }

    @Override
    public Object getLock(String key) {
        // 暂时只返回这个 key 本身作为锁对象（前提是 String 做了 intern，或者使用分段锁数组）
        // 简单实现：返回 ttlMap 或 memoryDb 全局锁 (性能较差但安全)
        // 优化实现：使用 Guava Striped 或者自己维护一个 ConcurrentHashMap<String, Object> locks
        return memoryDb;
    }
}
