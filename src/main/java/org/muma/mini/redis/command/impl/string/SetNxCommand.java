package org.muma.mini.redis.command.impl.string;

import org.muma.mini.redis.command.RedisCommand;
import org.muma.mini.redis.common.RedisData;
import org.muma.mini.redis.common.RedisDataType;
import org.muma.mini.redis.protocol.*;
import org.muma.mini.redis.server.RedisContext;
import org.muma.mini.redis.store.StorageEngine;

public class SetNxCommand implements RedisCommand {
    @Override
    public RedisMessage execute(StorageEngine storage,RedisArray args, RedisContext context) {
        // 格式: SETNX key value
        if (args.elements().length != 3) {
            return new ErrorMessage("ERR wrong number of arguments for 'setnx' command");
        }

        String key = ((BulkString) args.elements()[1]).asString();
        byte[] value = ((BulkString) args.elements()[2]).content();

        // 必须原子操作
        synchronized (storage.getLock(key)) {
            // 检查是否存在 (storage.get 返回 RedisData<?>)
            if (storage.get(key) != null) {
                return new RedisInteger(0); // 失败，Key 已存在
            }

            // 【修正核心】使用泛型构造 RedisData<byte[]>
            RedisData<byte[]> data = new RedisData<>(RedisDataType.STRING, value);

            storage.put(key, data);
            return new RedisInteger(1); // 成功
        }
    }
}
