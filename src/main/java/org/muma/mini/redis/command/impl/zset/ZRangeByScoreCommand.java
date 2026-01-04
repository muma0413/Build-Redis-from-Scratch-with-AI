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
import org.muma.mini.redis.store.structure.impl.zset.RangeSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.muma.mini.redis.store.structure.impl.zset.RangeSpec.parse;

public class ZRangeByScoreCommand implements RedisCommand {

    private static final Logger log = LoggerFactory.getLogger(ZRangeByScoreCommand.class);

    @Override
    public RedisMessage execute(StorageEngine storage, RedisArray args) {
        RedisMessage[] elements = args.elements();
        if (elements.length < 4) {
            return new ErrorMessage("ERR wrong number of arguments for 'zrangebyscore' command");
        }

        String key = ((BulkString) elements[1]).asString();
        String minStr = ((BulkString) elements[2]).asString();
        String maxStr = ((BulkString) elements[3]).asString();
        boolean withScores = false;
        int offset = 0;
        int count = Integer.MAX_VALUE;

        // 1. 解析 Range
        RangeSpec range;
        try {
            // 这里建议复用 ZCountCommand 中的静态方法，或者提取到 Utils
            range = parse(minStr, maxStr);
        } catch (IllegalArgumentException e) {
            return new ErrorMessage("ERR value is not a valid float");
        }

        // 2. 解析可选参数 (WITHSCORES, LIMIT)
        for (int i = 4; i < elements.length; i++) {
            String arg = ((BulkString) elements[i]).asString().toUpperCase();
            if ("WITHSCORES".equals(arg)) {
                withScores = true;
            } else if ("LIMIT".equals(arg)) {
                if (i + 2 >= elements.length) {
                    return new ErrorMessage("ERR syntax error");
                }
                try {
                    assert ((BulkString) elements[i + 1]).asString() != null;
                    offset = Integer.parseInt(((BulkString) elements[i + 1]).asString());
                    assert ((BulkString) elements[i + 2]).asString() != null;
                    count = Integer.parseInt(((BulkString) elements[i + 2]).asString());
                    i += 2; // 跳过 offset 和 count
                } catch (NumberFormatException e) {
                    return new ErrorMessage("ERR value is not an integer or out of range");
                }
            } else {
                return new ErrorMessage("ERR syntax error");
            }
        }

        // 3. 获取数据
        RedisData<?> data = storage.get(key);
        if (data == null) return new RedisArray(new RedisMessage[0]);
        if (data.getType() != RedisDataType.ZSET)
            return new ErrorMessage("WRONGTYPE Operation against a key holding the wrong kind of value");

        RedisZSet zset = data.getValue(RedisZSet.class);

        // 4. 执行查询
        List<RedisZSet.ZSetEntry> result = zset.rangeByScore(range, offset, count);

        if (result.size() > 10000) {
            log.warn("Large Result Alert: ZRANGEBYSCORE key={} returned {} items.", key, result.size());
        }

        // 5. 构建响应 (复用 buildResponse 逻辑，减少代码重复)
        return buildZSetResponse(result, withScores);
    }


}
