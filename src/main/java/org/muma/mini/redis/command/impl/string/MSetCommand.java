package org.muma.mini.redis.command.impl.string;

import org.muma.mini.redis.command.RedisCommand;
import org.muma.mini.redis.common.RedisData;
import org.muma.mini.redis.common.RedisDataType;
import org.muma.mini.redis.protocol.*;
import org.muma.mini.redis.server.RedisContext;
import org.muma.mini.redis.store.StorageEngine;

public class MSetCommand implements RedisCommand {
    @Override
    public RedisMessage execute(StorageEngine storage,RedisArray args, RedisContext context) {
        // 格式: MSET key value [key value ...]
        RedisMessage[] elements = args.elements();
        if ((elements.length - 1) % 2 != 0) {
            return new ErrorMessage("ERR wrong number of arguments for 'mset' command");
        }

        int pairCount = (elements.length - 1) / 2;

        // Redis 的 MSET 是原子的。
        // 在 Mini-Redis 简单实现中，为了保证绝对原子性，我们锁住 storage 对象
        synchronized (storage) {
            for (int i = 0; i < pairCount; i++) {
                String key = ((BulkString) elements[1 + i * 2]).asString();
                byte[] value = ((BulkString) elements[2 + i * 2]).content();

                // 【修正核心】使用泛型构造，明确数据类型为 byte[]
                RedisData<byte[]> data = new RedisData<>(RedisDataType.STRING, value);

                storage.put(key, data);
            }
        }

        return new SimpleString("OK");
    }
}
