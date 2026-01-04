package org.muma.mini.redis.command.impl.string;

import org.muma.mini.redis.command.RedisCommand;
import org.muma.mini.redis.common.RedisData;
import org.muma.mini.redis.common.RedisDataType;
import org.muma.mini.redis.protocol.*;
import org.muma.mini.redis.server.RedisContext;
import org.muma.mini.redis.store.StorageEngine;

import java.util.Locale;
import java.util.Objects;

public class SetCommand implements RedisCommand {

    private static final String OK = "OK";

    @Override
    public RedisMessage execute(StorageEngine storage,RedisArray args, RedisContext context) {
        // 基本格式: SET key value [NX|XX] [EX seconds | PX milliseconds]
        RedisMessage[] elements = args.elements();
        if (elements.length < 3) {
            return new ErrorMessage("ERR wrong number of arguments for 'set' command");
        }

        String key = ((BulkString) elements[1]).asString();
        byte[] value = ((BulkString) elements[2]).content();

        // --- 1. 参数解析阶段 ---
        boolean nx = false; // Not Exist
        boolean xx = false; // Already Exist
        long expireAt = -1; // 过期时间戳 (-1 表示不过期)

        // 从第3个参数开始解析选项
        for (int i = 3; i < elements.length; i++) {
            String opt = ((BulkString) elements[i]).asString().toUpperCase(Locale.ROOT);
            switch (opt) {
                case "NX" -> {
                    if (xx) return new ErrorMessage("ERR syntax error");
                    nx = true;
                }
                case "XX" -> {
                    if (nx) return new ErrorMessage("ERR syntax error");
                    xx = true;
                }
                case "EX" -> {
                    if (expireAt != -1) return new ErrorMessage("ERR syntax error");
                    if (i + 1 >= elements.length) return new ErrorMessage("ERR syntax error");
                    try {
                        long seconds = Long.parseLong(Objects.requireNonNull(((BulkString) elements[++i]).asString()));
                        if (seconds <= 0) return new ErrorMessage("ERR invalid expire time in set");
                        expireAt = System.currentTimeMillis() + (seconds * 1000);
                    } catch (NumberFormatException e) {
                        return new ErrorMessage("ERR value is not an integer or out of range");
                    }
                }
                case "PX" -> {
                    if (expireAt != -1) return new ErrorMessage("ERR syntax error");
                    if (i + 1 >= elements.length) return new ErrorMessage("ERR syntax error");
                    try {
                        long millis = Long.parseLong(Objects.requireNonNull(((BulkString) elements[++i]).asString()));
                        if (millis <= 0) return new ErrorMessage("ERR invalid expire time in set");
                        expireAt = System.currentTimeMillis() + millis;
                    } catch (NumberFormatException e) {
                        return new ErrorMessage("ERR value is not an integer or out of range");
                    }
                }
                default -> {
                    return new ErrorMessage("ERR syntax error");
                }
            }
        }

        // --- 2. 逻辑检查阶段 (NX/XX) ---
        // 为了保证原子性，如果使用了锁机制，这里需要在锁内检查。
        // 目前简单实现，假设单线程或 storage 内部处理并发。
        // 但 NX/XX 这种 check-and-set 逻辑，如果不加锁，在 MemoryStorageEngine 中可能存在竞态。
        // 既然是 Mini-Redis，我们可以先用 synchronized(storage) 或 storage.getLock(key) 锁住

        synchronized (storage.getLock(key)) {
            RedisData<?> existing = storage.get(key);

            if (nx && existing != null) {
                return new BulkString((byte[]) null); // Key 存在，NX 条件不满足，返回 Nil
            }
            if (xx && existing == null) {
                return new BulkString((byte[]) null); // Key 不存在，XX 条件不满足，返回 Nil
            }

            // --- 3. 写入阶段 ---
            RedisData<byte[]> newData = new RedisData<>(RedisDataType.STRING, value);
            if (expireAt != -1) {
                newData.setExpireAt(expireAt);
            }

            storage.put(key, newData);
        }

        return new SimpleString(OK);
    }
}
