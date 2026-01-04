package org.muma.mini.redis.command.impl.set;

import org.muma.mini.redis.command.RedisCommand;
import org.muma.mini.redis.common.RedisData;
import org.muma.mini.redis.common.RedisDataType;
import org.muma.mini.redis.common.RedisSet;
import org.muma.mini.redis.protocol.*;
import org.muma.mini.redis.server.RedisContext;
import org.muma.mini.redis.store.StorageEngine;

/**
 * SCARD key
 * Time Complexity: O(1)
 */
public class SCardCommand implements RedisCommand {
    @Override
    public RedisMessage execute(StorageEngine storage, RedisArray args, RedisContext context) {
        if (args.elements().length != 2) return errorArgs("scard");

        String key = ((BulkString) args.elements()[1]).asString();
        RedisData<?> data = storage.get(key);

        if (data == null) return new RedisInteger(0);
        if (data.getType() != RedisDataType.SET) return new ErrorMessage("WRONGTYPE Operation against a key holding the wrong kind of value");

        RedisSet set = data.getValue(RedisSet.class);
        return new RedisInteger(set.size());
    }
}
