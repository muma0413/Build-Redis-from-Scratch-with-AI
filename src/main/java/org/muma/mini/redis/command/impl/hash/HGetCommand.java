package org.muma.mini.redis.command.impl.hash;

import org.muma.mini.redis.command.RedisCommand;
import org.muma.mini.redis.common.RedisData;
import org.muma.mini.redis.common.RedisDataType;
import org.muma.mini.redis.common.RedisHash;
import org.muma.mini.redis.protocol.*;
import org.muma.mini.redis.server.RedisContext;
import org.muma.mini.redis.store.StorageEngine;

public class HGetCommand implements RedisCommand {
    @Override
    public RedisMessage execute(StorageEngine storage, RedisArray args, RedisContext context) {
        if (args.elements().length != 3) {
            return new ErrorMessage("ERR wrong number of arguments for 'hget' command");
        }

        String key = ((BulkString) args.elements()[1]).asString();
        String field = ((BulkString) args.elements()[2]).asString();

        RedisData<?> redisData = storage.get(key);

        if (redisData == null) {
            return new BulkString((byte[]) null);
        }

        if (redisData.getType() != RedisDataType.HASH) {
            return new ErrorMessage("WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        RedisHash hash = redisData.getValue(RedisHash.class);
        byte[] value = hash.get(field);

        return new BulkString(value); // value 为 null 时会自动处理为 Nil
    }
}
