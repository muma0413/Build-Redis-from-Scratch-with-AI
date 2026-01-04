package org.muma.mini.redis.command.impl.hash;

import org.muma.mini.redis.command.RedisCommand;
import org.muma.mini.redis.common.RedisData;
import org.muma.mini.redis.common.RedisDataType;
import org.muma.mini.redis.common.RedisHash;
import org.muma.mini.redis.protocol.*;
import org.muma.mini.redis.server.RedisContext;
import org.muma.mini.redis.store.StorageEngine;

/**
 * HLEN key
 * 时间复杂度: O(1)
 */
public class HLenCommand implements RedisCommand {
    @Override
    public RedisMessage execute(StorageEngine storage, RedisArray args, RedisContext context) {
        if (args.elements().length != 2) return new ErrorMessage("ERR wrong number of arguments for 'hlen'");

        String key = ((BulkString) args.elements()[1]).asString();
        RedisData<?> data = storage.get(key);

        if (data == null) return new RedisInteger(0);
        if (data.getType() != RedisDataType.HASH)
            return new ErrorMessage("WRONGTYPE Operation against a key holding the wrong kind of value");

        RedisHash hash = data.getValue(RedisHash.class);
        return new RedisInteger(hash.size());
    }
}
