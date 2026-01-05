package org.muma.mini.redis.command.impl.zset;

import org.muma.mini.redis.command.RedisCommand;
import org.muma.mini.redis.common.RedisData;
import org.muma.mini.redis.common.RedisDataType;
import org.muma.mini.redis.common.RedisZSet;
import org.muma.mini.redis.protocol.BulkString;
import org.muma.mini.redis.protocol.ErrorMessage;
import org.muma.mini.redis.protocol.RedisArray;
import org.muma.mini.redis.protocol.RedisMessage;
import org.muma.mini.redis.server.RedisContext;
import org.muma.mini.redis.store.StorageEngine;

/**
 * ZINCRBY key increment member
 * <p>
 * Time Complexity: O(log(N))
 * <p>
 * 功能：为有序集 key 的成员 member 的 score 值加上增量 increment。
 * 如果 key 不存在，创建一个空有序集并执行 ZADD。
 * 如果 member 不存在，则新增该 member，score 为 increment。
 */
public class ZIncrByCommand implements RedisCommand {
    @Override
    public RedisMessage execute(StorageEngine storage, RedisArray args, RedisContext context) {
        // 参数校验
        if (args.elements().length != 4) {
            return errorArgs("zincrby");
        }

        String key = ((BulkString) args.elements()[1]).asString();
        double increment;
        try {
            assert ((BulkString) args.elements()[2]).asString() != null;
            increment = Double.parseDouble(((BulkString) args.elements()[2]).asString());
            if (Double.isNaN(increment)) throw new NumberFormatException("NaN");
        } catch (NumberFormatException e) {
            return new ErrorMessage("ERR value is not a valid float");
        }
        String member = ((BulkString) args.elements()[3]).asString();

        // 1. 获取或创建 ZSet
        RedisData<?> data = storage.get(key);
        RedisZSet zset;

        if (data == null) {
            zset = new RedisZSet();
            data = new RedisData<>(RedisDataType.ZSET, zset);
        } else {
            if (data.getType() != RedisDataType.ZSET) {
                return new ErrorMessage("WRONGTYPE Operation against a key holding the wrong kind of value");
            }
            zset = data.getValue(RedisZSet.class);
        }

        // 2. 执行自增 (底层自动处理 ZipList/SkipList 逻辑)
        // 时间复杂度 O(logN)
        double newScore = zset.incrBy(increment, member);

        // 3. 回写
        storage.put(key, data);

        // 4. 返回新分数 (BulkString)
        String scoreStr = (newScore % 1 == 0) ?
                String.valueOf((long) newScore) : String.valueOf(newScore);
        return new BulkString(scoreStr);
    }
}
