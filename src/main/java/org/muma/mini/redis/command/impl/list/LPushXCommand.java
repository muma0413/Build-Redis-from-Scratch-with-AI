package org.muma.mini.redis.command.impl.list;

import org.muma.mini.redis.command.RedisCommand;
import org.muma.mini.redis.common.RedisData;
import org.muma.mini.redis.common.RedisDataType;
import org.muma.mini.redis.common.RedisList;
import org.muma.mini.redis.protocol.*;
import org.muma.mini.redis.server.RedisContext;
import org.muma.mini.redis.store.StorageEngine;

/**
 * LPUSHX key element [element ...]
 *
 * 【时间复杂度】 O(K)
 * 仅当 key 存在时才推入。
 */
public class LPushXCommand implements RedisCommand {
    @Override
    public RedisMessage execute(StorageEngine storage,RedisArray args, RedisContext context) {
        RedisMessage[] elements = args.elements();
        if (elements.length < 3) return errorArgs("lpushx");

        String key = ((BulkString) elements[1]).asString();

        synchronized (storage.getLock(key)) {
            RedisData<?> data = storage.get(key);

            // 核心差异：如果 Key 不存在，什么都不做，直接返回 0
            if (data == null) return new RedisInteger(0);

            if (data.getType() != RedisDataType.LIST) {
                return new ErrorMessage("WRONGTYPE Operation against a key holding the wrong kind of value");
            }

            RedisList list = data.getValue(RedisList.class);

            for (int i = 2; i < elements.length; i++) {
                byte[] val = ((BulkString) elements[i]).content();
                list.lpush(val);
            }

            storage.put(key, data);
            return new RedisInteger(list.size());
        }
    }
}
