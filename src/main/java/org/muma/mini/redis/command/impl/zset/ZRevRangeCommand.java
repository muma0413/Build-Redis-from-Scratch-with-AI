package org.muma.mini.redis.command.impl.zset;

import org.muma.mini.redis.command.RedisCommand;
import org.muma.mini.redis.common.RedisData;
import org.muma.mini.redis.common.RedisDataType;
import org.muma.mini.redis.common.RedisZSet;
import org.muma.mini.redis.protocol.BulkString;
import org.muma.mini.redis.protocol.ErrorMessage;
import org.muma.mini.redis.protocol.RedisArray;
import org.muma.mini.redis.protocol.RedisMessage;
import org.muma.mini.redis.store.StorageEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class ZRevRangeCommand implements RedisCommand {

    private static final Logger log = LoggerFactory.getLogger(ZRevRangeCommand.class);

    private static final int WARNING_THRESHOLD = 10000;

    @Override
    public RedisMessage execute(StorageEngine storage, RedisArray args) {
        RedisMessage[] elements = args.elements();
        if (elements.length < 4) {
            return new ErrorMessage("ERR wrong number of arguments for 'zrevrange' command");
        }

        String key = ((BulkString) elements[1]).asString();
        long start, stop;
        boolean withScores = false;

        // 1. 解析参数
        try {
            assert ((BulkString) elements[2]).asString() != null;
            start = Long.parseLong(((BulkString) elements[2]).asString());
            assert ((BulkString) elements[3]).asString() != null;
            stop = Long.parseLong(((BulkString) elements[3]).asString());
        } catch (NumberFormatException e) {
            return new ErrorMessage("ERR value is not an integer or out of range");
        }

        if (elements.length > 4) {
            if ("WITHSCORES".equalsIgnoreCase(((BulkString) elements[4]).asString())) {
                withScores = true;
            } else {
                return new ErrorMessage("ERR syntax error");
            }
        }

        // 2. 获取数据
        RedisData<?> data = storage.get(key);
        if (data == null) return new RedisArray(new RedisMessage[0]);
        if (data.getType() != RedisDataType.ZSET)
            return new ErrorMessage("WRONGTYPE Operation against a key holding the wrong kind of value");

        RedisZSet zset = data.getValue(RedisZSet.class);

        // 3. 核心查询
        // 防御性检查可以放在这里，也可以放在 RedisZSet 内部，放在这里对命令层更透明
        // checkThreshold(zset.size(), start, stop);

        List<RedisZSet.ZSetEntry> result = zset.revRange(start, stop);

        // 4. 构建响应
        return buildZSetResponse(result, withScores);
    }

}
