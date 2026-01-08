package org.muma.mini.redis.server;

import lombok.Getter;
import org.muma.mini.redis.aof.AofLoader;
import org.muma.mini.redis.aof.AofManager;
import org.muma.mini.redis.command.CommandDispatcher;
import org.muma.mini.redis.config.MiniRedisConfig;
import org.muma.mini.redis.rdb.RdbManager;
import org.muma.mini.redis.store.StorageEngine;
import org.muma.mini.redis.store.impl.MemoryStorageEngine;

/**
 * Redis 服务器上下文 (God Object)
 * 负责组装各个模块，管理生命周期。
 */
public class RedisServerContext {

    private final MiniRedisConfig config;
    private final StorageEngine storage;
    private final AofManager aofManager;
    private final RdbManager rdbManager;
    // --- Getters ---
    @Getter
    private final CommandDispatcher dispatcher;
    @Getter
    private final RedisCoreExecutor coreExecutor;

    public RedisServerContext(MiniRedisConfig config) {
        this.config = config;

        // 1. Storage
        MemoryStorageEngine memStorage = new MemoryStorageEngine();
        this.storage = memStorage;

        // 2. AOF & RDB Managers
        this.aofManager = new AofManager(config, storage);
        this.rdbManager = new RdbManager(config, storage);

        // 注入依赖 (Setter Injection)
        memStorage.setAofManager(aofManager);

        // 3. Dispatcher
        this.dispatcher = new CommandDispatcher(storage, aofManager);

        // 4. Core Executor
        this.coreExecutor = new RedisCoreExecutor();
    }

    /**
     * 核心初始化流程
     * 顺序非常重要：加载数据 -> 启动后台任务
     */
    public void init() {
        // Step 1: 尝试从 AOF 恢复数据
        // (如果有 AOF，优先用 AOF，否则用 RDB - 暂未实现 RDB Loader，只做 AOF)
        if (config.isAppendOnly()) {
            AofLoader loader = new AofLoader(config, dispatcher, storage);
            loader.load();
            aofManager.init(); // 打开文件准备写入
        }

        // Step 2: 启动 RDB 定时任务
        rdbManager.init();

        // Step 3: 注册 Shutdown Hook
        Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown));
    }

    public void shutdown() {
        aofManager.shutdown();
        // rdbManager.shutdown(); // 如果有需要
        // coreExecutor.shutdown();
    }

}
