package org.muma.mini.redis.command.impl.zset;

import org.muma.mini.redis.command.RedisCommand;
import org.muma.mini.redis.common.RedisData;
import org.muma.mini.redis.common.RedisDataType;
import org.muma.mini.redis.common.RedisZSet;
import org.muma.mini.redis.protocol.*;
import org.muma.mini.redis.store.StorageEngine;

import java.util.List;

/**
 * 命令: ZRANGE key start stop [WITHSCORES]
 * 功能: 返回有序集 key 中，指定区间内的成员。
 * <p>
 * 【时间复杂度 / Time Complexity】
 * O(log(N) + M)
 * - N: 有序集合的元素数量
 * - M: 返回的元素数量
 * <p>
 * 【复杂度分析】
 * 1. 定位 Start 节点: O(logN)。SkipList 通过跨度(span)快速跳跃定位到 Rank = start 的节点。
 * 2. 遍历 M 个节点: O(M)。定位到起点后，沿着 Level 0 的单向链表向后遍历 M 次。
 * 3. 总体复杂度为 O(logN + M)。
 * 注意：如果 M 很大（例如全量查询），此命令会退化为 O(N)。
 */
public class ZRangeCommand implements RedisCommand {
    @Override
    public RedisMessage execute(StorageEngine storage, RedisArray args) {
        RedisMessage[] elements = args.elements();
        if (elements.length < 4) {
            return new ErrorMessage("ERR wrong number of arguments for 'zrange' command");
        }

        String key = ((BulkString) elements[1]).asString();
        long start, stop;
        boolean withScores = false;

        try {
            assert ((BulkString) elements[2]).asString() != null;
            start = Long.parseLong(((BulkString) elements[2]).asString());
            assert ((BulkString) elements[3]).asString() != null;
            stop = Long.parseLong(((BulkString) elements[3]).asString());
        } catch (NumberFormatException e) {
            return new ErrorMessage("ERR value is not an integer or out of range");
        }

        // 解析选项
        if (elements.length > 4) {
            String opt = ((BulkString) elements[4]).asString();
            if ("WITHSCORES".equalsIgnoreCase(opt)) {
                withScores = true;
            } else {
                return new ErrorMessage("ERR syntax error");
            }
        }

        RedisData<?> data = storage.get(key);
        if (data == null) return new RedisArray(new RedisMessage[0]);
        if (data.getType() != RedisDataType.ZSET) {
            return new ErrorMessage("WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        RedisZSet zset = data.getValue(RedisZSet.class);

        // 调用 RedisZSet 的 range 方法 (底层会自动处理 ZipList 或 SkipList)
        List<RedisZSet.ZSetEntry> range = zset.range(start, stop);

        // 构建返回
        int size = range.size() * (withScores ? 2 : 1);
        RedisMessage[] result = new RedisMessage[size];

        int i = 0;
        for (RedisZSet.ZSetEntry entry : range) {
            result[i++] = new BulkString(entry.member());
            if (withScores) {
                String scoreStr = (entry.score() % 1 == 0) ?
                        String.valueOf((long) entry.score()) : String.valueOf(entry.score());
                result[i++] = new BulkString(scoreStr);
            }
        }

        return new RedisArray(result);
    }
}
