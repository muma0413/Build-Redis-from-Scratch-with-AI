package org.muma.mini.redis.command.impl.list;

import org.muma.mini.redis.command.RedisCommand;
import org.muma.mini.redis.common.RedisData;
import org.muma.mini.redis.common.RedisDataType;
import org.muma.mini.redis.common.RedisList;
import org.muma.mini.redis.protocol.*;
import org.muma.mini.redis.server.RedisContext;
import org.muma.mini.redis.store.StorageEngine;

/**
 * RPOP key
 *
 * 【时间复杂度】 O(1)
 */
public class RPopCommand implements RedisCommand {
    @Override
    public RedisMessage execute(StorageEngine storage,RedisArray args, RedisContext context) {
        if (args.elements().length != 2) return errorArgs("rpop");

        String key = ((BulkString) args.elements()[1]).asString();

        synchronized (storage.getLock(key)) {
            RedisData<?> data = storage.get(key);
            if (data == null) return new BulkString((byte[]) null);
            if (data.getType() != RedisDataType.LIST) {
                return new ErrorMessage("WRONGTYPE Operation against a key holding the wrong kind of value");
            }

            RedisList list = data.getValue(RedisList.class);
            byte[] val = list.rpop();

            if (list.size() == 0) {
                storage.remove(key);
            } else {
                storage.put(key, data);
            }

            return new BulkString(val);
        }
    }
}
