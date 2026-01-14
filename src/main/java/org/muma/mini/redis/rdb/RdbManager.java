package org.muma.mini.redis.rdb;

import org.muma.mini.redis.config.MiniRedisConfig;
import org.muma.mini.redis.store.StorageEngine;
import org.muma.mini.redis.utils.ThreadUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class RdbManager {

    private static final Logger log = LoggerFactory.getLogger(RdbManager.class);

    private final MiniRedisConfig config;
    private final StorageEngine storage;
    private final RdbSaver saver;
    private final ScheduledExecutorService cronExecutor;

    public RdbManager(MiniRedisConfig config, StorageEngine storage) {
        this.config = config;
        this.storage = storage;
        this.saver = new RdbSaver(storage);

        // ServerCron: 每 100ms 检查一次
        this.cronExecutor = Executors.newSingleThreadScheduledExecutor(
                ThreadUtils.namedThreadFactory("RDB-Cron")
        );
    }

    public void init() {
        if (config.getSaveParams().isEmpty()) return;
        cronExecutor.scheduleAtFixedRate(this::serverCron, 100, 100, TimeUnit.MILLISECONDS);
    }

    private void serverCron() {
        long dirty = storage.getDirty();
        long lastSave = storage.getLastSaveTime();
        long now = System.currentTimeMillis();

        for (MiniRedisConfig.SaveParam param : config.getSaveParams()) {
            if (dirty >= param.changes && (now - lastSave) > (param.seconds * 1000)) {
                log.info("RDB save triggered: {} changes in {} seconds", dirty, (now - lastSave) / 1000);
                triggerBgSave(); // 默认无回调
                break;
            }
        }
    }

    // --- 核心方法 ---

    /**
     * 触发后台保存 (默认无额外回调)
     */
    public void triggerBgSave() {
        triggerBgSave(null);
    }

    /**
     * 触发后台保存 (带回调，用于 Replication)
     */
    public void triggerBgSave(Consumer<File> callback) {
        // 【修改】统一使用 Config 获取路径
        File file = config.getRdbFile();

        // 确保父目录存在 (如果 RDB 和 AOF 不在一起，且目录未建)
        File parent = file.getParentFile();
        if (parent != null && !parent.exists()) parent.mkdirs();

        saver.bgsave(file, () -> {
            // 核心逻辑：重置 dirty
            storage.resetDirty();
            log.info("RDB bgsave finished: {}", file.getName());

            // 执行额外回调
            if (callback != null) {
                try {
                    callback.accept(file);
                } catch (Exception e) {
                    log.error("RDB callback failed", e);
                }
            }
        });
    }

    /**
     * 触发同步保存 (SAVE 命令)
     */
    public void triggerSave() {
        File file = config.getRdbFile(); // 【修改】统一路径
        // 确保父目录
        File parent = file.getParentFile();
        if (parent != null && !parent.exists()) parent.mkdirs();

        try {
            saver.save(file);
            storage.resetDirty();
        } catch (Exception e) {
            log.error("Save failed", e);
        }
    }
}
