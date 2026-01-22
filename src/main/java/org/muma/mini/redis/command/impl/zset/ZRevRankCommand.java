package org.muma.mini.redis.command.impl.zset;

import org.muma.mini.redis.command.RedisCommand;
import org.muma.mini.redis.common.RedisData;
import org.muma.mini.redis.common.RedisDataType;
import org.muma.mini.redis.common.RedisZSet;
import org.muma.mini.redis.protocol.*;
import org.muma.mini.redis.server.RedisContext;
import org.muma.mini.redis.store.StorageEngine;

/**
 * ZREVRANK key member
 * 返回有序集中指定成员的排名。其中有序集成员按 score 值递减(从大到小)顺序排列。
 * Time Complexity: O(logN) (利用正向 Rank 转换)
 */
public class ZRevRankCommand implements RedisCommand {
    @Override
    public RedisMessage execute(StorageEngine storage, RedisArray args, RedisContext context) {
        if (args.elements().length != 3) return errorArgs("zrevrank");

        String key = ((BulkString) args.elements()[1]).asString();
        String member = ((BulkString) args.elements()[2]).asString();

        RedisData<?> data = storage.get(key);
        if (data == null) return new BulkString((byte[]) null);
        if (data.getType() != RedisDataType.ZSET) return new ErrorMessage("WRONGTYPE Operation against a key holding the wrong kind of value");

        RedisZSet zset = data.getValue(RedisZSet.class);

        // 1. 获取正向排名
        Long rank = zset.getRank(member);
        if (rank == null) return new BulkString((byte[]) null);

        // 2. 转换为反向排名
        // Total: N, Rank: 0 -> RevRank: N-1
        long revRank = zset.size() - 1 - rank;

        return new RedisInteger(revRank);
    }
}
