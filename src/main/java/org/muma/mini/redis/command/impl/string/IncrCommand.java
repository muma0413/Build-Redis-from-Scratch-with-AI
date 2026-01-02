package org.muma.mini.redis.command.impl.string;

import org.muma.mini.redis.command.RedisCommand;
import org.muma.mini.redis.common.RedisData;
import org.muma.mini.redis.common.RedisDataType;
import org.muma.mini.redis.protocol.*;
import org.muma.mini.redis.store.StorageEngine;

import java.nio.charset.StandardCharsets;

public class IncrCommand implements RedisCommand {
    @Override
    public RedisMessage execute(StorageEngine storage, RedisArray args) {
        if (args.elements().length != 2) {
            return new ErrorMessage("ERR wrong number of arguments for 'incr' command");
        }

        String key = ((BulkString) args.elements()[1]).asString();

        // 获取锁，保证并发下的原子性 (Atomic)
        synchronized (storage.getLock(key)) {
            // 1. 使用通配符接收
            RedisData<?> data = storage.get(key);
            long val = 0;

            if (data != null) {
                if (data.getType() != RedisDataType.STRING) {
                    return new ErrorMessage("WRONGTYPE Operation against a key holding the wrong kind of value");
                }
                try {
                    // 2. 【修正核心】安全获取泛型数据
                    byte[] bytes = data.getValue(byte[].class);
                    String strVal = new String(bytes, StandardCharsets.UTF_8);
                    val = Long.parseLong(strVal);
                } catch (NumberFormatException e) {
                    return new ErrorMessage("ERR value is not an integer or out of range");
                }
            }

            val++;

            // 存回 DB
            String newValStr = String.valueOf(val);

            // 3. 【修正核心】泛型构造
            RedisData<byte[]> newData = new RedisData<>(RedisDataType.STRING, newValStr.getBytes(StandardCharsets.UTF_8));

            // 4. 【关键】保留原有的过期时间 (INCR 不会清除 TTL)
            if (data != null) {
                newData.setExpireAt(data.getExpireAt());
            }

            storage.put(key, newData);

            return new RedisInteger(val);
        }
    }
}
