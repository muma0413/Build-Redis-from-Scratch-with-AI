package org.muma.mini.redis.command.impl.string;

import org.muma.mini.redis.command.RedisCommand;
import org.muma.mini.redis.common.RedisData;
import org.muma.mini.redis.common.RedisDataType;
import org.muma.mini.redis.protocol.*;
import org.muma.mini.redis.server.RedisContext;
import org.muma.mini.redis.store.StorageEngine;

import java.util.Arrays;

/**
 * APPEND key value
 * <p>
 * 【时间复杂度】 O(1) 摊还复杂度 (因为需要拷贝内存，最坏 O(N))
 */
public class AppendCommand implements RedisCommand {
    @Override
    public RedisMessage execute(StorageEngine storage, RedisArray args, RedisContext context) {
        if (args.elements().length != 3) return errorArgs("append");

        String key = ((BulkString) args.elements()[1]).asString();
        byte[] value = ((BulkString) args.elements()[2]).content();

        RedisData<?> data = storage.get(key);

        // 1. 如果不存在，直接 SET
        if (data == null) {
            RedisData<byte[]> newData = new RedisData<>(RedisDataType.STRING, value);
            storage.put(key, newData);
            return new RedisInteger(value.length);
        }

        // 2. 类型检查
        if (data.getType() != RedisDataType.STRING) {
            return new ErrorMessage("WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        // 3. 执行追加
        byte[] oldBytes = data.getValue(byte[].class);
        byte[] newBytes = Arrays.copyOf(oldBytes, oldBytes.length + value.length);
        System.arraycopy(value, 0, newBytes, oldBytes.length, value.length);

        // 4. 更新
        RedisData<byte[]> updatedData = new RedisData<>(RedisDataType.STRING, newBytes);
        updatedData.setExpireAt(data.getExpireAt()); // 保持 TTL
        storage.put(key, updatedData);

        return new RedisInteger(newBytes.length);
    }

    @Override
    public boolean isWrite() {
        return true;
    }
}
