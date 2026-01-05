package org.muma.mini.redis.command.impl.list;

import org.muma.mini.redis.command.RedisCommand;
import org.muma.mini.redis.common.RedisData;
import org.muma.mini.redis.common.RedisDataType;
import org.muma.mini.redis.common.RedisList;
import org.muma.mini.redis.protocol.*;
import org.muma.mini.redis.server.RedisContext;
import org.muma.mini.redis.store.StorageEngine;

/**
 * LSET key index element
 * <p>
 * 【时间复杂度】 O(N)
 * N 是 index 的偏移量。
 */
public class LSetCommand implements RedisCommand {
    @Override
    public RedisMessage execute(StorageEngine storage, RedisArray args, RedisContext context) {
        if (args.elements().length != 4) return errorArgs("lset");

        String key = ((BulkString) args.elements()[1]).asString();
        long index;
        try {
            assert ((BulkString) args.elements()[2]).asString() != null;
            index = Long.parseLong(((BulkString) args.elements()[2]).asString());
        } catch (NumberFormatException e) {
            return errorInt();
        }
        byte[] element = ((BulkString) args.elements()[3]).content();

        RedisData<?> data = storage.get(key);
        if (data == null) return new ErrorMessage("ERR no such key");
        if (data.getType() != RedisDataType.LIST) {
            return new ErrorMessage("WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        RedisList list = data.getValue(RedisList.class);
        try {
            list.set(index, element);
        } catch (IndexOutOfBoundsException e) {
            return new ErrorMessage("ERR index out of range");
        }

        // 显式回写
        storage.put(key, data);

        return new SimpleString("OK");
    }
}
