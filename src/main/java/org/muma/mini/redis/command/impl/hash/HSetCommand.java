package org.muma.mini.redis.command.impl.hash;

import org.muma.mini.redis.command.RedisCommand;
import org.muma.mini.redis.common.RedisData;
import org.muma.mini.redis.common.RedisDataType;
import org.muma.mini.redis.common.RedisHash;
import org.muma.mini.redis.protocol.*;
import org.muma.mini.redis.server.RedisContext;
import org.muma.mini.redis.store.StorageEngine;

public class HSetCommand implements RedisCommand {
    @Override
    public RedisMessage execute(StorageEngine storage, RedisArray args, RedisContext context) {
        // 标准格式: HSET key field value [field value ...]
        RedisMessage[] elements = args.elements();

        // 参数校验：必须是 Key + 成对的 FV，所以总长度减去命令名(1)和Key(1)后必须是偶数
        if (elements.length < 4 || (elements.length - 2) % 2 != 0) {
            return new ErrorMessage("ERR wrong number of arguments for 'hset' command");
        }

        String key = ((BulkString) elements[1]).asString();
        int createdCount = 0;

        RedisData<?> redisData = storage.get(key);
        RedisHash hash;

        if (redisData == null) {
            hash = new RedisHash();
            redisData = new RedisData<>(RedisDataType.HASH, hash);
        } else {
            if (redisData.getType() != RedisDataType.HASH) {
                return new ErrorMessage("WRONGTYPE Operation against a key holding the wrong kind of value");
            }
            hash = redisData.getValue(RedisHash.class);
        }

        // 循环处理每一对 field-value
        for (int i = 2; i < elements.length; i += 2) {
            String field = ((BulkString) elements[i]).asString();
            byte[] value = ((BulkString) elements[i + 1]).content();

            // put 返回 1 表示新字段，0 表示更新
            createdCount += hash.put(field, value);
        }

        storage.put(key, redisData);

        return new RedisInteger(createdCount);
    }

    @Override
    public boolean isWrite() {
        return true;
    }
}
