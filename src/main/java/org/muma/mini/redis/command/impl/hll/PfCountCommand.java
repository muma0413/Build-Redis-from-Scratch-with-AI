package org.muma.mini.redis.command.impl.hll;

import org.muma.mini.redis.command.RedisCommand;
import org.muma.mini.redis.common.HyperLogLog;
import org.muma.mini.redis.common.RedisData;
import org.muma.mini.redis.common.RedisDataType;
import org.muma.mini.redis.protocol.*;
import org.muma.mini.redis.server.RedisContext;
import org.muma.mini.redis.store.StorageEngine;

/**
 * PFCOUNT key [key ...]
 */
public class PfCountCommand implements RedisCommand {
    @Override
    public RedisMessage execute(StorageEngine storage, RedisArray args, RedisContext context) {
        if (args.elements().length < 2) return errorArgs("pfcount");

        // 1. 如果只有一个 key
        if (args.elements().length == 2) {
            String key = ((BulkString) args.elements()[1]).asString();
            RedisData<?> data = storage.get(key);
            if (data == null) return new RedisInteger(0);
            if (data.getType() != RedisDataType.STRING) return new ErrorMessage("WRONGTYPE");

            byte[] registers = data.getValue(byte[].class);
            return new RedisInteger(HyperLogLog.count(registers));
        }

        // 2. 多个 key (Merge and Count)
        byte[] tempRegisters = new byte[256];
        for (int i = 1; i < args.elements().length; i++) {
            String key = ((BulkString) args.elements()[i]).asString();
            RedisData<?> data = storage.get(key);
            if (data != null) {
                if (data.getType() != RedisDataType.STRING) return new ErrorMessage("WRONGTYPE");
                byte[] src = data.getValue(byte[].class);
                HyperLogLog.merge(tempRegisters, src);
            }
        }
        return new RedisInteger(HyperLogLog.count(tempRegisters));
    }
}
