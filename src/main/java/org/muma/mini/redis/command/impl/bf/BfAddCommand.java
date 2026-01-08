package org.muma.mini.redis.command.impl.bf;

import org.muma.mini.redis.command.RedisCommand;
import org.muma.mini.redis.common.BloomFilter;
import org.muma.mini.redis.common.RedisData;
import org.muma.mini.redis.common.RedisDataType;
import org.muma.mini.redis.protocol.*;
import org.muma.mini.redis.server.RedisContext;
import org.muma.mini.redis.store.StorageEngine;

import java.nio.ByteBuffer;

/**
 * BF.ADD key item
 */
public class BfAddCommand implements RedisCommand {
    @Override
    public RedisMessage execute(StorageEngine storage, RedisArray args, RedisContext context) {
        if (args.elements().length != 3) return errorArgs("bf.add");

        String key = ((BulkString) args.elements()[1]).asString();
        byte[] item = ((BulkString) args.elements()[2]).content();

        synchronized (storage.getLock(key)) {
            RedisData<?> data = storage.get(key);
            byte[] bytes;
            int m = 1024 * 8; // Default
            int k = 5;

            if (data == null) {
                // 自动创建默认 BF
                int byteLen = (m + 7) / 8;
                bytes = new byte[8 + byteLen];
                ByteBuffer.wrap(bytes).putInt(m).putInt(k);
                data = new RedisData<>(RedisDataType.STRING, bytes);
                storage.put(key, (RedisData<byte[]>) data);
            } else {
                if (data.getType() != RedisDataType.STRING) {
                    return new ErrorMessage("WRONGTYPE");
                }
                bytes = data.getValue(byte[].class);
                if (bytes.length < 8) return new ErrorMessage("ERR not a valid BloomFilter");
                ByteBuffer buf = ByteBuffer.wrap(bytes);
                m = buf.getInt();
                k = buf.getInt();
            }

            int[] positions = BloomFilter.getHashPositions(item, m, k);
            boolean updated = false;

            for (int pos : positions) {
                // Header 8 bytes
                int byteIdx = 8 + (pos / 8);
                int bitIdx = pos % 8;

                int oldByte = bytes[byteIdx];
                int mask = 1 << (7 - bitIdx);

                if ((oldByte & mask) == 0) {
                    updated = true;
                    bytes[byteIdx] |= mask;
                }
            }

            // 已经是引用修改，但为了 AOF/RDB 钩子，put 一下
            // storage.put(key, data);

            return new RedisInteger(updated ? 1 : 0);
        }
    }

    @Override
    public boolean isWrite() { return true; }
}
