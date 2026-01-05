package org.muma.mini.redis.command.impl.list;

import org.muma.mini.redis.command.RedisCommand;
import org.muma.mini.redis.common.RedisData;
import org.muma.mini.redis.common.RedisDataType;
import org.muma.mini.redis.common.RedisList;
import org.muma.mini.redis.protocol.*;
import org.muma.mini.redis.server.RedisContext;
import org.muma.mini.redis.store.StorageEngine;

/**
 * LTRIM key start stop
 * <p>
 * 【时间复杂度】 O(N)
 * N 是被移除元素的数量。
 */
public class LTrimCommand implements RedisCommand {
    @Override
    public RedisMessage execute(StorageEngine storage, RedisArray args, RedisContext context) {
        if (args.elements().length != 4) return errorArgs("ltrim");

        String key = ((BulkString) args.elements()[1]).asString();
        long start, stop;
        try {
            assert ((BulkString) args.elements()[2]).asString() != null;
            start = Long.parseLong(((BulkString) args.elements()[2]).asString());
            assert ((BulkString) args.elements()[3]).asString() != null;
            stop = Long.parseLong(((BulkString) args.elements()[3]).asString());
        } catch (NumberFormatException e) {
            return errorInt();
        }

        RedisData<?> data = storage.get(key);
        if (data == null) return new SimpleString("OK"); // Key 不存在视为成功
        if (data.getType() != RedisDataType.LIST)
            return new ErrorMessage("WRONGTYPE Operation against a key holding the wrong kind of value");

        RedisList list = data.getValue(RedisList.class);
        list.trim(start, stop);

        if (list.size() == 0) storage.remove(key);
        else storage.put(key, data);

        return new SimpleString("OK");
    }
}
