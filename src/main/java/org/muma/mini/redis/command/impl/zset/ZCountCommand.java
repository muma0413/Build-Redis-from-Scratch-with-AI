package org.muma.mini.redis.command.impl.zset;

import org.muma.mini.redis.command.RedisCommand;
import org.muma.mini.redis.common.RedisData;
import org.muma.mini.redis.common.RedisDataType;
import org.muma.mini.redis.common.RedisZSet;
import org.muma.mini.redis.protocol.*;
import org.muma.mini.redis.server.RedisContext;
import org.muma.mini.redis.store.StorageEngine;
import org.muma.mini.redis.store.structure.impl.zset.RangeSpec;

import static org.muma.mini.redis.store.structure.impl.zset.RangeSpec.parse;

/**
 * ZCOUNT key min max
 * <p>
 * Time Complexity: O(log(N))
 * 利用 SkipList 的 Rank 特性，直接计算数量，无需遍历。
 */
public class ZCountCommand implements RedisCommand {

    @Override
    public RedisMessage execute(StorageEngine storage,RedisArray args, RedisContext context) {
        if (args.elements().length != 4) {
            return new ErrorMessage("ERR wrong number of arguments for 'zcount' command");
        }

        String key = ((BulkString) args.elements()[1]).asString();
        String minStr = ((BulkString) args.elements()[2]).asString();
        String maxStr = ((BulkString) args.elements()[3]).asString();

        RangeSpec range;
        try {
            range = parse(minStr, maxStr);
        } catch (IllegalArgumentException e) {
            return new ErrorMessage("ERR value is not a valid float");
        }

        RedisData<?> data = storage.get(key);
        if (data == null) return new RedisInteger(0);
        if (data.getType() != RedisDataType.ZSET) {
            return new ErrorMessage("WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        RedisZSet zset = data.getValue(RedisZSet.class);

        // 核心调用：O(logN)
        long count = zset.count(range);

        return new RedisInteger(count);
    }

}
