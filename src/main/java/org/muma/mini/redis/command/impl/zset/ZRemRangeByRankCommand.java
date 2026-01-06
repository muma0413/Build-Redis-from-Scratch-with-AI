package org.muma.mini.redis.command.impl.zset;

import org.muma.mini.redis.command.RedisCommand;
import org.muma.mini.redis.common.RedisData;
import org.muma.mini.redis.common.RedisDataType;
import org.muma.mini.redis.common.RedisZSet;
import org.muma.mini.redis.protocol.*;
import org.muma.mini.redis.server.RedisContext;
import org.muma.mini.redis.store.StorageEngine;

/**
 * ZREMRANGEBYRANK key start stop
 */
public class ZRemRangeByRankCommand implements RedisCommand {
    @Override
    public RedisMessage execute(StorageEngine storage, RedisArray args, RedisContext context) {
        if (args.elements().length != 4) return errorArgs("zremrangebyrank");

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

        if (data == null) return new RedisInteger(0);
        if (data.getType() != RedisDataType.ZSET)
            return new ErrorMessage("WRONGTYPE Operation against a key holding the wrong kind of value");

        RedisZSet zset = data.getValue(RedisZSet.class);
        int removed = zset.removeRange(start, stop);

        if (zset.size() == 0) storage.remove(key);
        else storage.put(key, data);

        return new RedisInteger(removed);
    }

    @Override
    public boolean isWrite() {
        return true;
    }
}
