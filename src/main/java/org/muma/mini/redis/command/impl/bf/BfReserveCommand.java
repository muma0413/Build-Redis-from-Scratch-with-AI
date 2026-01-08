package org.muma.mini.redis.command.impl.bf;

import org.muma.mini.redis.command.RedisCommand;
import org.muma.mini.redis.common.RedisData;
import org.muma.mini.redis.common.RedisDataType;
import org.muma.mini.redis.protocol.*;
import org.muma.mini.redis.server.RedisContext;
import org.muma.mini.redis.store.StorageEngine;

import java.nio.ByteBuffer;

/**
 * BF.RESERVE key error_rate capacity
 *
 * 简化：BF.RESERVE key m k
 * (为了不用算 error_rate 公式，我们直接让用户传 bits 和 hashes，或者内部转换)
 * 假设 args: key m k
 */
public class BfReserveCommand implements RedisCommand {
    @Override
    public RedisMessage execute(StorageEngine storage, RedisArray args, RedisContext context) {
        if (args.elements().length != 4) return errorArgs("bf.reserve");

        String key = ((BulkString) args.elements()[1]).asString();
        int m, k;
        try {
            m = Integer.parseInt(((BulkString) args.elements()[2]).asString());
            k = Integer.parseInt(((BulkString) args.elements()[3]).asString());
        } catch (NumberFormatException e) {
            return errorInt();
        }

        synchronized (storage.getLock(key)) {
            if (storage.get(key) != null) return new ErrorMessage("ERR item exists");

            // 格式: [m:4][k:4][bytes: (m+7)/8]
            int byteLen = (m + 7) / 8;
            byte[] data = new byte[8 + byteLen];

            ByteBuffer buf = ByteBuffer.wrap(data);
            buf.putInt(m);
            buf.putInt(k);

            storage.put(key, new RedisData<>(RedisDataType.STRING, data));
            return new SimpleString("OK");
        }
    }

    @Override
    public boolean isWrite() { return true; }
}
