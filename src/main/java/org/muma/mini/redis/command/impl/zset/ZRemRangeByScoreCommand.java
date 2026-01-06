package org.muma.mini.redis.command.impl.zset;

import org.muma.mini.redis.command.RedisCommand;
import org.muma.mini.redis.common.RedisData;
import org.muma.mini.redis.common.RedisDataType;
import org.muma.mini.redis.common.RedisZSet;
import org.muma.mini.redis.protocol.*;
import org.muma.mini.redis.server.RedisContext;
import org.muma.mini.redis.store.StorageEngine;
import org.muma.mini.redis.store.structure.impl.zset.RangeSpec;

/**
 * ZREMRANGEBYSCORE key min max
 */
public class ZRemRangeByScoreCommand implements RedisCommand {
    @Override
    public RedisMessage execute(StorageEngine storage, RedisArray args, RedisContext context) {
        if (args.elements().length != 4) return errorArgs("zremrangebyscore");

        String key = ((BulkString) args.elements()[1]).asString();
        String minStr = ((BulkString) args.elements()[2]).asString();
        String maxStr = ((BulkString) args.elements()[3]).asString();

        RangeSpec range;
        try {
            range = RangeSpec.parse(minStr, maxStr);
        } catch (IllegalArgumentException e) {
            return new ErrorMessage("ERR value is not a valid float");
        }

        RedisData<?> data = storage.get(key);
        if (data == null) return new RedisInteger(0);
        if (data.getType() != RedisDataType.ZSET)
            return new ErrorMessage("WRONGTYPE Operation against a key holding the wrong kind of value");

        RedisZSet zset = data.getValue(RedisZSet.class);
        int removed = zset.removeRangeByScore(range);

        if (zset.size() == 0) storage.remove(key);
        else storage.put(key, data);

        return new RedisInteger(removed);
    }

    @Override
    public boolean isWrite() {
        return true;
    }
}
