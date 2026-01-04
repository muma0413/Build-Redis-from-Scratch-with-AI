package org.muma.mini.redis.command.impl.hash;

import org.muma.mini.redis.command.RedisCommand;
import org.muma.mini.redis.common.RedisData;
import org.muma.mini.redis.common.RedisDataType;
import org.muma.mini.redis.common.RedisHash;
import org.muma.mini.redis.protocol.*;
import org.muma.mini.redis.server.RedisContext;
import org.muma.mini.redis.store.StorageEngine;

public class HMGetCommand implements RedisCommand {
    @Override
    public RedisMessage execute(StorageEngine storage, RedisArray args, RedisContext context) {
        // HMGET key field [field ...]
        if (args.elements().length < 3) {
            return new ErrorMessage("ERR wrong number of arguments for 'hmget' command");
        }

        String key = ((BulkString) args.elements()[1]).asString();
        RedisMessage[] results = new RedisMessage[args.elements().length - 2];

        RedisData<?> redisData = storage.get(key);

        // 即使 Key 不存在或类型不对，HMGET 也不会报错中断，而是对每个字段返回 nil
        // 但如果是类型不对，标准 Redis 还是会报 WRONGTYPE。我们这里遵循标准。
        if (redisData != null && redisData.getType() != RedisDataType.HASH) {
            return new ErrorMessage("WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        RedisHash hash = (redisData != null) ? redisData.getValue(RedisHash.class) : null;

        for (int i = 2; i < args.elements().length; i++) {
            String field = ((BulkString) args.elements()[i]).asString();
            byte[] value = (hash != null) ? hash.get(field) : null;

            // 字段不存在返回 nil
            results[i - 2] = new BulkString(value);
        }

        return new RedisArray(results);
    }
}
