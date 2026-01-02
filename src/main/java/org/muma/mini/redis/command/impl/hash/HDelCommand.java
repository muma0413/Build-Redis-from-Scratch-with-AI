package org.muma.mini.redis.command.impl.hash;

import org.muma.mini.redis.command.RedisCommand;
import org.muma.mini.redis.common.RedisData;
import org.muma.mini.redis.common.RedisDataType;
import org.muma.mini.redis.common.RedisHash;
import org.muma.mini.redis.protocol.*;
import org.muma.mini.redis.store.StorageEngine;

public class HDelCommand implements RedisCommand {
    @Override
    public RedisMessage execute(StorageEngine storage, RedisArray args) {
        // HDEL key field [field ...]
        RedisMessage[] elements = args.elements();
        if (elements.length < 3) {
            return new ErrorMessage("ERR wrong number of arguments for 'hdel' command");
        }

        String key = ((BulkString) elements[1]).asString();
        int deletedCount = 0;

        synchronized (storage.getLock(key)) {
            RedisData<?> redisData = storage.get(key);
            if (redisData == null) {
                return new RedisInteger(0);
            }
            if (redisData.getType() != RedisDataType.HASH) {
                return new ErrorMessage("WRONGTYPE Operation against a key holding the wrong kind of value");
            }

            RedisHash hash = redisData.getValue(RedisHash.class);

            // 循环删除多个 field
            for (int i = 2; i < elements.length; i++) {
                String field = ((BulkString) elements[i]).asString();
                deletedCount += hash.remove(field);
            }

            // 优化：如果 Hash 为空了，应该把整个 Key 从 DB 移除
            if (hash.size() == 0) {
                storage.remove(key);
            } else {
                // 显式通知更新 (虽然内存可能是引用的，但语义上需要)
                storage.put(key, redisData);
            }
        }

        return new RedisInteger(deletedCount);
    }
}
