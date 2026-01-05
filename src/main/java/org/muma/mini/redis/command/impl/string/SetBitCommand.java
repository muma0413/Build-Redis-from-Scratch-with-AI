package org.muma.mini.redis.command.impl.string;

import org.muma.mini.redis.command.RedisCommand;
import org.muma.mini.redis.common.RedisData;
import org.muma.mini.redis.common.RedisDataType;
import org.muma.mini.redis.protocol.*;
import org.muma.mini.redis.server.RedisContext;
import org.muma.mini.redis.store.StorageEngine;

import java.util.Arrays;

/**
 * SETBIT key offset value
 * Time: O(1)
 */
public class SetBitCommand implements RedisCommand {
    @Override
    public RedisMessage execute(StorageEngine storage, RedisArray args, RedisContext context) {
        if (args.elements().length != 4) return errorArgs("setbit");

        String key = ((BulkString) args.elements()[1]).asString();
        long offset;
        int value;
        try {
            assert ((BulkString) args.elements()[2]).asString() != null;
            offset = Long.parseLong(((BulkString) args.elements()[2]).asString());
            assert ((BulkString) args.elements()[3]).asString() != null;
            value = Integer.parseInt(((BulkString) args.elements()[3]).asString());
            if (offset < 0) return new ErrorMessage("ERR bit offset is not an integer or out of range");
            if (value != 0 && value != 1) return new ErrorMessage("ERR bit is not an integer or out of range");
        } catch (NumberFormatException e) {
            return new ErrorMessage("ERR bit offset is not an integer or out of range");
        }

        RedisData<?> data = storage.get(key);
        byte[] bytes;

        if (data == null) {
            bytes = new byte[0]; // 稍后扩容
        } else {
            if (data.getType() != RedisDataType.STRING) {
                return new ErrorMessage("WRONGTYPE Operation against a key holding the wrong kind of value");
            }
            bytes = data.getValue(byte[].class);
        }

        int byteIndex = (int) (offset / 8);
        int bitOffset = (int) (offset % 8);

        // 1. 扩容逻辑
        if (byteIndex >= bytes.length) {
            // 新长度需覆盖 byteIndex
            // Redis 可能会预分配，这里简单按需分配
            // 注意：如果 offset 很大 (如 2^32)，数组会 OOM，需限制 max offset。
            // Redis 限制 string 最大 512MB。我们这里暂不加硬限制，依靠 JVM 抛 OOM。
            bytes = Arrays.copyOf(bytes, byteIndex + 1);
        }

        // 2. 获取旧值 (用于返回)
        byte currentByte = bytes[byteIndex];
        int oldBit = (currentByte >> (7 - bitOffset)) & 1;

        // 3. 设置新值
        if (value == 1) {
            bytes[byteIndex] |= (1 << (7 - bitOffset));
        } else {
            bytes[byteIndex] &= ~(1 << (7 - bitOffset));
        }

        // 4. 存回
        RedisData<byte[]> newData = new RedisData<>(RedisDataType.STRING, bytes);
        if (data != null) newData.setExpireAt(data.getExpireAt());
        storage.put(key, newData);

        return new RedisInteger(oldBit);
    }
}
