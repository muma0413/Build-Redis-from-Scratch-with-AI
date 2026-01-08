package org.muma.mini.redis.rdb;

import org.muma.mini.redis.common.*;
import org.muma.mini.redis.store.StorageEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

public class RdbSaver {

    private static final Logger log = LoggerFactory.getLogger(RdbSaver.class);

    private final StorageEngine storage;
    private final ExecutorService bgsaveExecutor = Executors.newSingleThreadExecutor(r -> new Thread(r, "RDB-Bgsave-Worker"));
    private final AtomicBoolean isBgsaving = new AtomicBoolean(false);

    public RdbSaver(StorageEngine storage) {
        this.storage = storage;
    }

    /**
     * 阻塞式保存 (SAVE)
     */
    public void save(File file) throws IOException {
        long start = System.currentTimeMillis();
        // 临时文件，写完后 rename
        File tempFile = new File(file.getParent(), "temp-" + file.getName());

        try (BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(tempFile))) {
            RdbEncoder encoder = new RdbEncoder(bos);

            // 1. Header: REDIS0009
            encoder.writeBytes(RdbConstants.MAGIC);
            encoder.writeBytes(RdbConstants.VERSION.getBytes(StandardCharsets.UTF_8));

            // 2. Select DB 0
            encoder.writeByte(RdbConstants.OP_SELECTDB);
            encoder.writeLength(0); // DB ID

            // 3. 遍历 Key-Value
            for (String key : storage.keys()) {
                RedisData<?> data = storage.get(key);
                if (data == null || data.isExpired()) continue;

                // 3.1 写入过期时间 (如果有)
                long expireAt = data.getExpireAt();
                if (expireAt != -1) {
                    encoder.writeByte(RdbConstants.OP_EXPIRETIME_MS);
                    encoder.writeLong(expireAt);
                }

                // 3.2 写入类型
                int type = getRdbType(data.getType());
                encoder.writeByte(type);

                // 3.3 写入 Key
                encoder.writeString(key);

                // 3.4 写入 Value
                writeValue(encoder, data);
            }

            // 4. EOF
            encoder.writeByte(RdbConstants.OP_EOF);

            // 5. Checksum (暂略，写 8 个 0 或者计算 CRC64)
            // encoder.writeLong(0);

            bos.flush();
        }

        // Atomic Rename
        if (file.exists()) file.delete();
        if (!tempFile.renameTo(file)) {
            // Windows 兼容
            throw new IOException("Failed to rename temp RDB file");
        }

        long duration = System.currentTimeMillis() - start;
        log.info("DB saved on disk. Size: {}, Duration: {} ms", file.length(), duration);
    }

    /**
     * 后台保存 (BGSAVE)
     */
    public void bgsave(File file) {
        if (!isBgsaving.compareAndSet(false, true)) {
            log.warn("Background save already in progress");
            return;
        }

        bgsaveExecutor.submit(() -> {
            try {
                save(file);
            } catch (IOException e) {
                log.error("Background save failed", e);
            } finally {
                isBgsaving.set(false);
            }
        });
    }

    // --- Helpers ---

    private int getRdbType(RedisDataType type) {
        return switch (type) {
            case STRING -> RdbType.STRING;
            case LIST -> RdbType.LIST;
            case SET -> RdbType.SET;
            case HASH -> RdbType.HASH;
            case ZSET -> RdbType.ZSET;
            default -> throw new IllegalArgumentException("Unknown type: " + type);
        };
    }

    private void writeValue(RdbEncoder encoder, RedisData<?> data) throws IOException {
        switch (data.getType()) {
            case STRING -> encoder.writeString((byte[]) data.getData());
            case LIST -> encoder.writeList((RedisList) data.getData());
            case SET -> encoder.writeSet((RedisSet) data.getData());
            case HASH -> encoder.writeHash((RedisHash) data.getData());
            case ZSET -> encoder.writeZSet((RedisZSet) data.getData());
        }
    }
    public void bgsave(File file, Runnable onSuccess) {
        if (!isBgsaving.compareAndSet(false, true)) return;

        bgsaveExecutor.submit(() -> {
            try {
                save(file);
                if (onSuccess != null) onSuccess.run();
            } catch (Exception e) {
                log.error("Bgsave failed", e);
            } finally {
                isBgsaving.set(false);
            }
        });
    }

}
