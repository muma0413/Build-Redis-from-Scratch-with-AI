package org.muma.mini.redis.command.impl.list;

import org.muma.mini.redis.command.RedisCommand;
import org.muma.mini.redis.common.RedisData;
import org.muma.mini.redis.common.RedisDataType;
import org.muma.mini.redis.common.RedisList;
import org.muma.mini.redis.protocol.*;
import org.muma.mini.redis.server.RedisContext;
import org.muma.mini.redis.store.StorageEngine;

/**
 * LPUSH key element [element ...]
 * <p>
 * 【时间复杂度】 O(K)
 * K 是推入元素的数量。对于单个元素推入是 O(1)。
 */
public class LPushCommand implements RedisCommand {
    @Override
    public RedisMessage execute(StorageEngine storage, RedisArray args, RedisContext context) {
        RedisMessage[] elements = args.elements();
        if (elements.length < 3) return errorArgs("lpush");

        String key = ((BulkString) elements[1]).asString();

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

        // Redis 规定 LPUSH 多元素时，相当于依次 LPUSH
        // LPUSH mylist a b c -> c, b, a
        for (int i = 2; i < elements.length; i++) {
            byte[] val = ((BulkString) elements[i]).content();
            list.lpush(val);
        }

        storage.put(key, data);

        // ★★★ 这一行必须有！★★★
        storage.getBlockingManager().onPush(key, storage);
        return new RedisInteger(list.size());
    }
}
