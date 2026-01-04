package org.muma.mini.redis.command.impl.list;

import org.muma.mini.redis.command.RedisCommand;
import org.muma.mini.redis.common.RedisData;
import org.muma.mini.redis.common.RedisDataType;
import org.muma.mini.redis.common.RedisList;
import org.muma.mini.redis.protocol.*;
import org.muma.mini.redis.server.RedisContext;
import org.muma.mini.redis.store.StorageEngine;

import java.util.List;

/**
 * LRANGE key start stop
 * <p>
 * 【时间复杂度】 O(S+N)
 * S 是 start 偏移量，N 是指定区间内的元素数量。
 * 由于 QuickList 支持分段跳跃，实际 S 的开销会比纯 LinkedList 小。
 */
public class LRangeCommand implements RedisCommand {
    @Override
    public RedisMessage execute(StorageEngine storage, RedisArray args, RedisContext context) {
        if (args.elements().length != 4) return errorArgs("lrange");

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
        if (data == null) return new RedisArray(new RedisMessage[0]); // 空列表
        if (data.getType() != RedisDataType.LIST) {
            return new ErrorMessage("WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        RedisList list = data.getValue(RedisList.class);
        List<byte[]> items = list.range(start, stop);

        RedisMessage[] result = new RedisMessage[items.size()];
        for (int i = 0; i < items.size(); i++) {
            result[i] = new BulkString(items.get(i));
        }

        return new RedisArray(result);
    }
}
