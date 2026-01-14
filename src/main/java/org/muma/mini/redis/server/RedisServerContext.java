package org.muma.mini.redis.server;

import org.muma.mini.redis.aof.AofLoader;
import org.muma.mini.redis.aof.AofManager;
import org.muma.mini.redis.command.CommandDispatcher;
import org.muma.mini.redis.config.MiniRedisConfig;
import org.muma.mini.redis.rdb.RdbLoader;
import org.muma.mini.redis.rdb.RdbManager;
import org.muma.mini.redis.replication.ReplicationManager;
import org.muma.mini.redis.store.StorageEngine;
import org.muma.mini.redis.store.impl.MemoryStorageEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * Redis 服务器上下文 (God Object)
 */
public class RedisServerContext {

    private static final Logger log = LoggerFactory.getLogger(RedisServerContext.class);

    private final MiniRedisConfig config;
    private final StorageEngine storage;
    private final AofManager aofManager;
    private final RdbManager rdbManager;
    private final ReplicationManager replicationManager;
    private final CommandDispatcher dispatcher;
    private final RedisCoreExecutor coreExecutor;

    public RedisServerContext(MiniRedisConfig config) {
        this.config = config;

        // 打印核心配置摘要，方便调试
        log.info("Building Server Context | Port: {} | AOF: {} | Backend: {}",
                config.getPort(), config.isAppendOnly(), config.getSetDictBackend());

        // 1. Core Executor
        this.coreExecutor = new RedisCoreExecutor();

        // 2. Storage
        MemoryStorageEngine memStorage = new MemoryStorageEngine();
        this.storage = memStorage;

        // 3. Managers
        this.aofManager = new AofManager(config, storage);
        this.rdbManager = new RdbManager(config, storage);
        this.replicationManager = new ReplicationManager(config, storage, coreExecutor);

        // 注入 Storage 依赖
        memStorage.setAofManager(aofManager);

        // 4. Dispatcher
        this.dispatcher = new CommandDispatcher(storage, aofManager, replicationManager, rdbManager, this.config);

        // 解决循环依赖
        this.replicationManager.setDispatcher(this.dispatcher);
    }

    public void init() {
        log.info("RedisServerContext initializing...");

        try {
            // Step 1: 数据恢复
            restoreData();

            // Step 2: 启动 RDB 定时任务
            rdbManager.init();
            log.info("RDB Manager started.");

            // Step 3: 触发主从复制
            if (config.getSlaveOfHost() != null && config.getSlaveOfPort() > 0) {
                log.info("Auto-configuring SLAVEOF {} {}",
                        config.getSlaveOfHost(), config.getSlaveOfPort());
                replicationManager.slaveOf(config.getSlaveOfHost(), config.getSlaveOfPort());
            }

            // Step 4: 注册 Shutdown Hook
            Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown, "Redis-Shutdown-Hook"));
            log.info("Shutdown hook registered.");

        } catch (Exception e) {
            log.error("Fatal error during server initialization", e);
            throw new RuntimeException("Server init failed", e);
        }
    }

    private void restoreData() {
        // 检查路径是否存在
        File aofDir = config.getAofDirFile();
        if (!aofDir.exists()) {
            log.info("Data directory '{}' does not exist, creating...", aofDir.getAbsolutePath());
            aofDir.mkdirs();
        }

        if (config.isAppendOnly()) {
            log.info("AOF enabled. Looking for AOF files in: {}", aofDir.getAbsolutePath());
            try {
                // AofLoader 内部已经使用了 config.getAppendDir()
                // 确保 AofLoader 内部也是用 config.getAofDirFile() (如果之前没改的话)
                // 这里我们相信 AofLoader 的实现，只打印日志
                AofLoader loader = new AofLoader(config, dispatcher, storage);
                loader.load();
                aofManager.init();
                log.info("AOF recovery completed.");
            } catch (Exception e) {
                log.error("Failed to recover from AOF", e);
                throw new RuntimeException("AOF load failed", e);
            }
        } else {
            // 【修改】使用 config.getRdbFile()
            File rdbFile = config.getRdbFile();

            if (rdbFile.exists()) {
                log.info("RDB file found at: {}", rdbFile.getAbsolutePath());
                try {
                    new RdbLoader(storage).load(rdbFile);
                    log.info("RDB recovery completed.");
                } catch (Exception e) {
                    log.error("Failed to recover from RDB", e);
                }
            } else {
                log.info("No RDB file found at: {}. Starting with empty database.", rdbFile.getAbsolutePath());
            }
        }
    }

    public void shutdown() {
        log.info("Shutting down RedisServerContext...");
        aofManager.shutdown();
        // 其他组件如果需要优雅关闭，也在这里调用
        log.info("RedisServerContext shutdown complete.");
    }

    public CommandDispatcher getDispatcher() {
        return dispatcher;
    }

    public RedisCoreExecutor getCoreExecutor() {
        return coreExecutor;
    }
}
