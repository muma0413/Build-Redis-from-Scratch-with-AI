package org.muma.mini.redis.command.impl.list;

import org.muma.mini.redis.command.RedisCommand;
import org.muma.mini.redis.common.RedisData;
import org.muma.mini.redis.common.RedisDataType;
import org.muma.mini.redis.common.RedisList;
import org.muma.mini.redis.protocol.*;
import org.muma.mini.redis.server.RedisContext;
import org.muma.mini.redis.store.StorageEngine;

/**
 * RPUSHX key element [element ...]
 * <p>
 * 【时间复杂度】 O(K)
 */
public class RPushXCommand implements RedisCommand {
    @Override
    public RedisMessage execute(StorageEngine storage, RedisArray args, RedisContext context) {
        RedisMessage[] elements = args.elements();
        if (elements.length < 3) return errorArgs("rpushx");

        String key = ((BulkString) elements[1]).asString();

        RedisData<?> data = storage.get(key);

        if (data == null) return new RedisInteger(0);

        if (data.getType() != RedisDataType.LIST) {
            return new ErrorMessage("WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        RedisList list = data.getValue(RedisList.class);

        for (int i = 2; i < elements.length; i++) {
            byte[] val = ((BulkString) elements[i]).content();
            list.rpush(val);
        }

        storage.put(key, data);

        // 【新增】触发唤醒
        storage.getBlockingManager().onPush(key, storage);

        return new RedisInteger(list.size());
    }
}
