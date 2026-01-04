package org.muma.mini.redis.command.impl.string;

import org.muma.mini.redis.command.RedisCommand;
import org.muma.mini.redis.common.RedisData;
import org.muma.mini.redis.common.RedisDataType;
import org.muma.mini.redis.protocol.*;
import org.muma.mini.redis.server.RedisContext;
import org.muma.mini.redis.store.StorageEngine;

/**
 * BITCOUNT key [start end]
 * Time: O(N)
 */
public class BitCountCommand implements RedisCommand {
    @Override
    public RedisMessage execute(StorageEngine storage, RedisArray args, RedisContext context) {
        // BITCOUNT key [start end]
        if (args.elements().length != 2 && args.elements().length != 4) {
            return new ErrorMessage("ERR syntax error");
        }

        String key = ((BulkString) args.elements()[1]).asString();
        long start = 0;
        long end = -1;
        boolean hasRange = false;

        if (args.elements().length == 4) {
            try {
                assert ((BulkString) args.elements()[2]).asString() != null;
                start = Long.parseLong(((BulkString) args.elements()[2]).asString());
                assert ((BulkString) args.elements()[3]).asString() != null;
                end = Long.parseLong(((BulkString) args.elements()[3]).asString());
                hasRange = true;
            } catch (NumberFormatException e) {
                return errorInt();
            }
        }

        RedisData<?> data = storage.get(key);
        if (data == null) return new RedisInteger(0);
        if (data.getType() != RedisDataType.STRING)
            return new ErrorMessage("WRONGTYPE Operation against a key holding the wrong kind of value");

        byte[] bytes = data.getValue(byte[].class);

        // 处理 Range (注意：BITCOUNT 的 range 是按字节索引，不是按位)
        if (hasRange) {
            if (start < 0) start = bytes.length + start;
            if (end < 0) end = bytes.length + end;

            if (start < 0) start = 0;
            if (end >= bytes.length) end = bytes.length - 1;

            if (start > end) return new RedisInteger(0);
        } else {
            end = bytes.length - 1;
        }

        long count = 0;
        for (int i = (int) start; i <= end; i++) {
            // byte 转 int 时需要 & 0xFF 避免负数补码问题
            count += Integer.bitCount(bytes[i] & 0xFF);
        }

        return new RedisInteger(count);
    }
}
