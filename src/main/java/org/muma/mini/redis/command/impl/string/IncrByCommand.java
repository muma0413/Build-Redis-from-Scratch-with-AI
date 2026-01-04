package org.muma.mini.redis.command.impl.string;

import org.muma.mini.redis.command.RedisCommand;
import org.muma.mini.redis.common.RedisData;
import org.muma.mini.redis.common.RedisDataType;
import org.muma.mini.redis.protocol.*;
import org.muma.mini.redis.server.RedisContext;
import org.muma.mini.redis.store.StorageEngine;

import java.nio.charset.StandardCharsets;

/**
 * INCRBY key increment
 */
public class IncrByCommand implements RedisCommand {
    @Override
    public RedisMessage execute(StorageEngine storage, RedisArray args, RedisContext context) {
        if (args.elements().length != 3) return errorArgs("incrby");

        String key = ((BulkString) args.elements()[1]).asString();
        long increment;
        try {
            increment = Long.parseLong(((BulkString) args.elements()[2]).asString());
        } catch (NumberFormatException e) {
            return errorInt();
        }

        synchronized (storage.getLock(key)) {
            RedisData<?> data = storage.get(key);
            long val = 0;

            if (data != null) {
                if (data.getType() != RedisDataType.STRING) {
                    return new ErrorMessage("WRONGTYPE Operation against a key holding the wrong kind of value");
                }
                try {
                    byte[] bytes = data.getValue(byte[].class);
                    String strVal = new String(bytes, StandardCharsets.UTF_8);
                    val = Long.parseLong(strVal);
                } catch (NumberFormatException e) {
                    return errorInt(); // 字符串不是整数
                }
            }

            // 核心运算
            val += increment; // 支持负数，所以 DECRBY 其实也可以用这个逻辑

            String newValStr = String.valueOf(val);
            RedisData<byte[]> newData = new RedisData<>(RedisDataType.STRING, newValStr.getBytes(StandardCharsets.UTF_8));

            // 继承 TTL
            if (data != null) newData.setExpireAt(data.getExpireAt());

            storage.put(key, newData);
            return new RedisInteger(val);
        }
    }
}
