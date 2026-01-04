package org.muma.mini.redis.command.impl.string;

import org.muma.mini.redis.command.RedisCommand;
import org.muma.mini.redis.common.RedisData;
import org.muma.mini.redis.common.RedisDataType;
import org.muma.mini.redis.protocol.*;
import org.muma.mini.redis.server.RedisContext;
import org.muma.mini.redis.store.StorageEngine;

/**
 * GETBIT key offset
 * Time: O(1)
 */
public class GetBitCommand implements RedisCommand {
    @Override
    public RedisMessage execute(StorageEngine storage, RedisArray args, RedisContext context) {
        if (args.elements().length != 3) return errorArgs("getbit");

        String key = ((BulkString) args.elements()[1]).asString();
        long offset;
        try {
            offset = Long.parseLong(((BulkString) args.elements()[2]).asString());
            if (offset < 0) return new ErrorMessage("ERR bit offset is not an integer or out of range");
        } catch (NumberFormatException e) {
            return new ErrorMessage("ERR bit offset is not an integer or out of range");
        }

        RedisData<?> data = storage.get(key);
        if (data == null) return new RedisInteger(0); // 不存在视全为 0
        if (data.getType() != RedisDataType.STRING) return new ErrorMessage("WRONGTYPE Operation against a key holding the wrong kind of value");

        byte[] bytes = data.getValue(byte[].class);
        int byteIndex = (int) (offset / 8);

        // 越界视全为 0
        if (byteIndex >= bytes.length) {
            return new RedisInteger(0);
        }

        int bitOffset = (int) (offset % 8);
        int bit = (bytes[byteIndex] >> (7 - bitOffset)) & 1;

        return new RedisInteger(bit);
    }
}
