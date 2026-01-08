package org.muma.mini.redis.command.impl.bf;

import org.muma.mini.redis.command.RedisCommand;
import org.muma.mini.redis.common.BloomFilter;
import org.muma.mini.redis.common.RedisData;
import org.muma.mini.redis.common.RedisDataType;
import org.muma.mini.redis.protocol.*;
import org.muma.mini.redis.server.RedisContext;
import org.muma.mini.redis.store.StorageEngine;

import java.nio.ByteBuffer;

public class BfExistsCommand implements RedisCommand {
    @Override
    public RedisMessage execute(StorageEngine storage, RedisArray args, RedisContext context) {
        if (args.elements().length != 3) return errorArgs("bf.exists");

        String key = ((BulkString) args.elements()[1]).asString();
        byte[] item = ((BulkString) args.elements()[2]).content();

        // 读操作不需要全库锁，但为了防止 bitmap 扩容并发，如果是 ConcurrentHashMap 可以不锁，
        // 这里沿用惯例（如果 storage.get 本身线程安全）
        // 简单起见，不加 synchronized 块，直接 get

        RedisData<?> data = storage.get(key);
        if (data == null) return new RedisInteger(0);
        if (data.getType() != RedisDataType.STRING) return new ErrorMessage("WRONGTYPE");

        byte[] bytes = data.getValue(byte[].class);
        if (bytes.length < 8) return new ErrorMessage("ERR not a valid BloomFilter");

        ByteBuffer buf = ByteBuffer.wrap(bytes);
        int m = buf.getInt();
        int k = buf.getInt();

        int[] positions = BloomFilter.getHashPositions(item, m, k);

        for (int pos : positions) {
            int byteIdx = 8 + (pos / 8);
            int bitIdx = pos % 8;

            // 越界检查 (虽然理论上不会)
            if (byteIdx >= bytes.length) return new RedisInteger(0);

            int currentByte = bytes[byteIdx];
            int mask = 1 << (7 - bitIdx);

            if ((currentByte & mask) == 0) {
                return new RedisInteger(0); // 只要有一位是 0，肯定不存在
            }
        }

        return new RedisInteger(1); // 可能存在
    }
}
