package org.muma.mini.redis.command.impl.zset;

import org.muma.mini.redis.command.RedisCommand;
import org.muma.mini.redis.common.RedisData;
import org.muma.mini.redis.common.RedisDataType;
import org.muma.mini.redis.common.RedisZSet;
import org.muma.mini.redis.protocol.*;
import org.muma.mini.redis.server.RedisContext;
import org.muma.mini.redis.store.StorageEngine;

/**
 * ZRANK key member
 * 返回有序集中指定成员的排名。其中有序集成员按 score 值递增(从小到大)顺序排列。
 * Time Complexity: O(logN)
 * 排名以 0 为底。
 */
public class ZRankCommand implements RedisCommand {
    @Override
    public RedisMessage execute(StorageEngine storage, RedisArray args, RedisContext context) {
        if (args.elements().length != 3) return errorArgs("zrank");

        String key = ((BulkString) args.elements()[1]).asString();
        String member = ((BulkString) args.elements()[2]).asString();

        RedisData<?> data = storage.get(key);
        if (data == null) return new BulkString((byte[]) null);
        if (data.getType() != RedisDataType.ZSET)
            return new ErrorMessage("WRONGTYPE Operation against a key holding the wrong kind of value");

        RedisZSet zset = data.getValue(RedisZSet.class);

        // getRank 返回 0-based 排名，不存在返回 null
        Long rank = zset.getRank(member);

        return rank == null ? new BulkString((byte[]) null) : new RedisInteger(rank);
    }
}
