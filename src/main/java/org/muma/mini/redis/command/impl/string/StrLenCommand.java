package org.muma.mini.redis.command.impl.string;

import org.muma.mini.redis.command.RedisCommand;
import org.muma.mini.redis.common.RedisData;
import org.muma.mini.redis.common.RedisDataType;
import org.muma.mini.redis.protocol.*;
import org.muma.mini.redis.server.RedisContext;
import org.muma.mini.redis.store.StorageEngine;

/**
 * STRLEN key
 *
 * 【时间复杂度】 O(1)
 */
public class StrLenCommand implements RedisCommand {
    @Override
    public RedisMessage execute(StorageEngine storage, RedisArray args, RedisContext context) {
        if (args.elements().length != 2) return errorArgs("strlen");

        String key = ((BulkString) args.elements()[1]).asString();

        RedisData<?> data = storage.get(key);

        if (data == null) {
            return new RedisInteger(0); // Key 不存在视为长度 0
        }

        if (data.getType() != RedisDataType.STRING) {
            return new ErrorMessage("WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        byte[] bytes = data.getValue(byte[].class);
        return new RedisInteger(bytes.length);
    }
}
