package org.muma.mini.redis.server;

import lombok.Getter;
import org.muma.mini.redis.aof.AofLoader;
import org.muma.mini.redis.aof.AofManager;
import org.muma.mini.redis.command.CommandDispatcher;
import org.muma.mini.redis.config.MiniRedisConfig;
import org.muma.mini.redis.rdb.RdbLoader;
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
    /**
     * 核心初始化流程
     * 顺序：数据恢复 -> 启动后台任务 -> 注册关闭钩子
     */
    public void init() {
        boolean dataLoaded = false;

        // Step 1: 数据恢复 (Data Recovery)
        if (config.isAppendOnly()) {
            // AOF 开启：优先且只加载 AOF
            AofLoader loader = new AofLoader(config, dispatcher, storage);
            loader.load();
            aofManager.init(); // 打开文件准备追加
            dataLoaded = true;
        } else {
            // AOF 关闭：尝试加载 RDB
            // 注意：默认 RDB 文件名目前硬编码为 dump.rdb，建议后续加到 Config
            String rdbPath = config.getAppendDir(); // 假设 RDB 和 AOF 在同一目录
            java.io.File rdbFile = new java.io.File(rdbPath, "dump.rdb");

            if (rdbFile.exists()) {
                try {
                    new RdbLoader(storage).load(rdbFile);
                    dataLoaded = true;
                } catch (Exception e) {
                    // RDB 加载失败通常只打日志，不阻断启动
                    // log.error("Failed to load RDB", e);
                    e.printStackTrace();
                }
            }
        }

        // Step 2: 启动 RDB 定时任务 (BGSAVE Cron)
        // 无论是否从 RDB 恢复，只要配置了 save 规则，就需要启动 Cron
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
