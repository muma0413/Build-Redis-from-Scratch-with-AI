package org.muma.mini.redis.command.impl.list;

import org.muma.mini.redis.command.RedisCommand;
import org.muma.mini.redis.common.RedisData;
import org.muma.mini.redis.common.RedisDataType;
import org.muma.mini.redis.common.RedisList;
import org.muma.mini.redis.protocol.*;
import org.muma.mini.redis.server.RedisContext;
import org.muma.mini.redis.store.StorageEngine;

/**
 * RPUSH key element [element ...]
 * <p>
 * 【时间复杂度】 O(K)
 */
public class RPushCommand implements RedisCommand {
    @Override
    public RedisMessage execute(StorageEngine storage,RedisArray args, RedisContext context) {
        RedisMessage[] elements = args.elements();
        if (elements.length < 3) return errorArgs("rpush");

        String key = ((BulkString) elements[1]).asString();

        synchronized (storage.getLock(key)) {
            RedisData<?> data = storage.get(key);
            RedisList list;

            if (data == null) {
                list = new RedisList();
                data = new RedisData<>(RedisDataType.LIST, list);
            } else {
                if (data.getType() != RedisDataType.LIST) {
                    return new ErrorMessage("WRONGTYPE Operation against a key holding the wrong kind of value");
                }
                list = data.getValue(RedisList.class);
            }

            for (int i = 2; i < elements.length; i++) {
                byte[] val = ((BulkString) elements[i]).content();
                list.rpush(val);
            }

            storage.put(key, data);
            return new RedisInteger(list.size());
        }
    }
}
