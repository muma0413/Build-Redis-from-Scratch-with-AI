package org.muma.mini.redis.command.impl.zset;

import org.muma.mini.redis.command.RedisCommand;
import org.muma.mini.redis.common.RedisData;
import org.muma.mini.redis.common.RedisDataType;
import org.muma.mini.redis.common.RedisZSet;
import org.muma.mini.redis.protocol.*;
import org.muma.mini.redis.store.StorageEngine;

/**
 * 命令: ZSCORE key member
 * 功能: 返回有序集 key 中，成员 member 的 score 值。
 * <p>
 * 【时间复杂度 / Time Complexity】
 * O(1)
 * <p>
 * 【复杂度分析】
 * 1. 对于 SkipList: 内部维护了一个 HashMap (Dict)，所以查找 Score 是 O(1) 的哈希查找。
 * 2. 对于 ZipList: 虽然需要线性遍历 O(N)，但由于 N < 128，性能损耗微乎其微。
 * 3. 对外承诺复杂度为 O(1)。
 */
public class ZScoreCommand implements RedisCommand {
    @Override
    public RedisMessage execute(StorageEngine storage, RedisArray args) {
        if (args.elements().length != 3) {
            return new ErrorMessage("ERR wrong number of arguments for 'zscore' command");
        }

        String key = ((BulkString) args.elements()[1]).asString();
        String member = ((BulkString) args.elements()[2]).asString();

        RedisData<?> data = storage.get(key);

        // Key 不存在返回 nil
        if (data == null) return new BulkString((byte[]) null);

        // 类型错误
        if (data.getType() != RedisDataType.ZSET) {
            return new ErrorMessage("WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        RedisZSet zset = data.getValue(RedisZSet.class);
        Double score = zset.getScore(member);

        if (score == null) {
            return new BulkString((byte[]) null); // Member 不存在
        }

        // 格式化输出：如果是整数，去掉 .0
        String scoreStr = (score % 1 == 0) ?
                String.valueOf(score.longValue()) : String.valueOf(score);

        return new BulkString(scoreStr);
    }
}
