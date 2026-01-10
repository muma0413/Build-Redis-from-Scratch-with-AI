package org.muma.mini.redis.server;

import org.muma.mini.redis.aof.AofLoader;
import org.muma.mini.redis.aof.AofManager;
import org.muma.mini.redis.command.CommandDispatcher;
import org.muma.mini.redis.config.MiniRedisConfig;
import org.muma.mini.redis.rdb.RdbManager;
import org.muma.mini.redis.replication.ReplicationManager;
import org.muma.mini.redis.store.StorageEngine;
import org.muma.mini.redis.store.impl.MemoryStorageEngine;

public class RedisServerContext {

    private final MiniRedisConfig config;
    private final StorageEngine storage;
    private final AofManager aofManager;
    private final RdbManager rdbManager;
    private final ReplicationManager replicationManager; // 【新增】
    private final CommandDispatcher dispatcher;
    private final RedisCoreExecutor coreExecutor;

    public RedisServerContext(MiniRedisConfig config) {
        this.config = config;

        // 1. Core Executor (ReplicationManager 需要它来异步连接 Master)
        this.coreExecutor = new RedisCoreExecutor();

        // 2. Storage
        MemoryStorageEngine memStorage = new MemoryStorageEngine();
        this.storage = memStorage;

        // 3. Managers
        this.aofManager = new AofManager(config, storage);
        this.rdbManager = new RdbManager(config, storage);
        this.replicationManager = new ReplicationManager(config, storage, coreExecutor); // 【新增】

        // 注入 Storage 依赖
        memStorage.setAofManager(aofManager);
        // 如果 Storage 需要感知 Replication (比如写入时 Propagate)，也需要注入
        // memStorage.setReplicationManager(replicationManager);

        // 4. Dispatcher (全家桶注入)
        this.dispatcher = new CommandDispatcher(storage, aofManager, replicationManager, rdbManager);
    }

    public void init() {
        // Step 1: 数据恢复 (优先 AOF，兜底 RDB)
        if (config.isAppendOnly()) {
            // AOF 开启：只加载 AOF
            AofLoader loader = new AofLoader(config, dispatcher, storage);
            loader.load();
            aofManager.init();
        } else {
            // AOF 关闭：尝试加载 RDB
            java.io.File rdbFile = new java.io.File(config.getAppendDir(), config.getRdbFilename());
            if (rdbFile.exists()) {
                try {
                    new org.muma.mini.redis.rdb.RdbLoader(storage).load(rdbFile);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

        // Step 2: 启动 RDB 定时任务
        rdbManager.init();

        // Step 3: Shutdown Hook
        Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown));
    }


    public void shutdown() {
        aofManager.shutdown();
        // replicationManager.shutdown();
        // coreExecutor.shutdown();
    }

    public CommandDispatcher getDispatcher() {
        return dispatcher;
    }

    public RedisCoreExecutor getCoreExecutor() {
        return coreExecutor;
    }
}
