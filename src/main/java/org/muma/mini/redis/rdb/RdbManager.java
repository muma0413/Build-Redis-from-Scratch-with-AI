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

        // ServerCron: 每 100ms 检查一次 (和 Redis 默认 serverCron 频率一致)
        this.cronExecutor = Executors.newSingleThreadScheduledExecutor(
                ThreadUtils.namedThreadFactory("RDB-Cron")
        );
    }

    public void init() {
        if (config.getSaveParams().isEmpty()) return;

        cronExecutor.scheduleAtFixedRate(this::serverCron, 100, 100, TimeUnit.MILLISECONDS);
    }

    private void serverCron() {
        // 检查是否满足任意一个 save 条件
        long dirty = storage.getDirty();
        long lastSave = storage.getLastSaveTime();
        long now = System.currentTimeMillis();

        for (MiniRedisConfig.SaveParam param : config.getSaveParams()) {
            if (dirty >= param.changes &&
                    (now - lastSave) > (param.seconds * 1000)) {

                log.info("RDB save triggered: {} changes in {} seconds", dirty, (now-lastSave)/1000);
                triggerBgsave();
                break; // 触发一次即可
            }
        }
    }

    public void triggerBgsave() {
        File file = new File(config.getAppendDir(), config.getRdbFilename()); // 这里假设 RDB 和 AOF 存一起
        // Callback: 保存成功后重置 dirty
        saver.bgsave(file, storage::resetDirty);
    }

    public void triggerSave() {
        // 同步保存
        File file = new File(config.getAppendDir(), config.getRdbFilename());
        try {
            saver.save(file);
            storage.resetDirty();
        } catch (Exception e) {
            log.error("Save failed", e);
        }
    }
}
