package org.muma.mini.redis.store.impl;

import lombok.Setter;
import org.muma.mini.redis.aof.AofManager;
import org.muma.mini.redis.common.RedisData;
import org.muma.mini.redis.protocol.RedisArray;
import org.muma.mini.redis.server.BlockingManager;
import org.muma.mini.redis.store.StorageEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class MemoryStorageEngine implements StorageEngine {

    private static final Logger log = LoggerFactory.getLogger(MemoryStorageEngine.class);

    // 核心数据存储
    private final Map<String, RedisData<?>> memoryDb = new ConcurrentHashMap<>();

    // 过期时间管理 (TTL)
    private final Map<String, Long> ttlMap = new ConcurrentHashMap<>();

    // AOF 管理器引用
    @Setter
    private AofManager aofManager;

    // 阻塞管理器
    private final org.muma.mini.redis.server.BlockingManager blockingManager = new org.muma.mini.redis.server.BlockingManager();

    // RDB 统计
    private final AtomicLong dirty = new AtomicLong(0);
    private volatile long lastSaveTime = System.currentTimeMillis();

    // 定期清理线程池
    private final ScheduledExecutorService cleanupExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "Redis-Active-Cleanup");
        t.setDaemon(true);
        return t;
    });

    public MemoryStorageEngine() {
        // 启动定期清理任务
        cleanupExecutor.scheduleAtFixedRate(this::activeExpireCycle, 1, 100, TimeUnit.MILLISECONDS);
    }

    @Override
    public RedisData<?> get(String key) {
        RedisData<?> data = memoryDb.get(key);
        if (data == null) return null;

        if (data.isExpired()) {
            remove(key);
            return null;
        }
        return data;
    }

    @Override
    public void put(String key, RedisData<?> data) {
        memoryDb.put(key, data);

        // 更新 TTL 索引
        if (data.getExpireAt() != -1) {
            ttlMap.put(key, data.getExpireAt());
        } else {
            ttlMap.remove(key);
        }

        // 增加 dirty 计数 (用于 RDB 触发)
        dirty.incrementAndGet();
    }

    @Override
    public boolean remove(String key) {
        ttlMap.remove(key);
        boolean removed = memoryDb.remove(key) != null;
        if (removed) {
            dirty.incrementAndGet();
        }
        return removed;
    }

    @Override
    public void flush() {
        memoryDb.clear();
        ttlMap.clear();
        dirty.incrementAndGet(); // Flush 算一次巨大的修改
    }

    @Override
    public Object getLock(String key) {
        // 单线程架构下不再需要锁，保留此方法兼容接口
        // 如果为了某些细粒度操作需要，返回内部 map 本身即可
        return memoryDb;
    }

    @Override
    public void appendAof(RedisArray command) {
        if (aofManager != null) {
            aofManager.append(command);
        }
    }

    @Override
    public Iterable<String> keys() {
        return memoryDb.keySet();
    }

    @Override
    public BlockingManager getBlockingManager() {
        return blockingManager;
    }

    // --- RDB 支持 ---

    @Override
    public long getDirty() {
        return dirty.get();
    }

    @Override
    public long getLastSaveTime() {
        return lastSaveTime;
    }

    @Override
    public void resetDirty() {
        dirty.set(0);
        lastSaveTime = System.currentTimeMillis();
    }

    // --- 内部逻辑 ---

    private void activeExpireCycle() {
        if (ttlMap.isEmpty()) return;

        // 每次随机抽查 20 个
        int sampleSize = 20;
        int expiredCount = 0;
        long now = System.currentTimeMillis();

        Iterator<Map.Entry<String, Long>> iterator = ttlMap.entrySet().iterator();
        int loop = 0;

        while (iterator.hasNext() && loop < sampleSize) {
            Map.Entry<String, Long> entry = iterator.next();
            if (now > entry.getValue()) {
                memoryDb.remove(entry.getKey());
                iterator.remove();
                expiredCount++;
                dirty.incrementAndGet(); // 过期删除也算修改
            }
            loop++;
        }

        if (expiredCount > 5) {
            // 如果过期比例高，可以尝试立即再跑一次 (简化逻辑暂不实现)
        }
    }
}
