package org.muma.mini.redis.rdb;

import org.muma.mini.redis.common.RedisData;
import org.muma.mini.redis.common.RedisDataType;
import org.muma.mini.redis.store.StorageEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;

public class RdbLoader {

    private static final Logger log = LoggerFactory.getLogger(RdbLoader.class);
    private final StorageEngine storage;

    public RdbLoader(StorageEngine storage) {
        this.storage = storage;
    }

    public void load(File file) throws IOException {
        if (!file.exists()) return;

        log.info("Loading RDB file: {}", file.getName());
        long start = System.currentTimeMillis();
        int count = 0;

        try (BufferedInputStream bis = new BufferedInputStream(new FileInputStream(file))) {
            RdbDecoder decoder = new RdbDecoder(bis);

            // 1. Check Magic "REDIS"
            byte[] magic = decoder.readBytes(5);
            if (!Arrays.equals(magic, RdbConstants.MAGIC)) {
                throw new IOException("Invalid RDB file: Bad Magic");
            }

            // 2. Read Version (4 bytes)
            byte[] version = decoder.readBytes(4);
            // 这里可以校验版本，暂略

            // 3. Loop Opcodes
            long expireAt = -1; // 当前 Key 的过期时间

            while (true) {
                int type = decoder.readByte();

                if (type == RdbConstants.OP_EOF) {
                    break;
                } else if (type == RdbConstants.OP_SELECTDB) {
                    decoder.readLength(); // DB ID, ignore
                    continue;
                } else if (type == RdbConstants.OP_EXPIRETIME_MS) {
                    expireAt = decoder.readLong(); // 读 8 字节时间戳
                    continue; // 继续读下一个字节，那是 Type
                } else if (type == RdbConstants.OP_EXPIRETIME) {
                    // 秒级时间戳转毫秒
                    expireAt = (long)decoder.readInt() * 1000;
                    continue;
                } else if (type == RdbConstants.OP_RESIZEDB || type == RdbConstants.OP_AUX) {
                    // 辅助字段，跳过 (需根据协议解析跳过，简化版暂不支持 AUX，假设没有)
                    // 如果遇到 AUX 可能会解析错位。
                    // 暂且假设我们只读自己生成的 RDB (没有 AUX)
                    continue;
                }

                // --- 读 Key-Value ---
                // 此时 type 是 ValueType (0..4)
                String key = decoder.readStringUtf8();
                RedisData<?> data = readValue(decoder, type);

                if (expireAt != -1) {
                    data.setExpireAt(expireAt);
                    expireAt = -1; // 重置
                }

                // 存入内存
                // 如果已过期，是否要存？Redis 策略是加载时检查过期。
                if (!data.isExpired()) {
                    storage.put(key, data);
                    count++;
                }
            }
        }

        long duration = System.currentTimeMillis() - start;
        log.info("RDB loaded. Keys: {}, Duration: {} ms", count, duration);
    }

    private RedisData<?> readValue(RdbDecoder decoder, int type) throws IOException {
        return switch (type) {
            case RdbType.STRING -> new RedisData<>(RedisDataType.STRING, decoder.readString());
            case RdbType.LIST -> new RedisData<>(RedisDataType.LIST, decoder.readList());
            case RdbType.SET -> new RedisData<>(RedisDataType.SET, decoder.readSet());
            case RdbType.HASH -> new RedisData<>(RedisDataType.HASH, decoder.readHash());
            case RdbType.ZSET -> new RedisData<>(RedisDataType.ZSET, decoder.readZSet());
            default -> throw new IOException("Unknown value type: " + type);
        };
    }

    // readInt 辅助：因为 decoder.readLength 返回 long，readExpire 需要 int
    // 其实 decoder 里有 readInt，需要 public
}
