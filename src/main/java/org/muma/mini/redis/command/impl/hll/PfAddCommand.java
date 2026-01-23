package org.muma.mini.redis.command.impl.hll;

import org.muma.mini.redis.command.RedisCommand;
import org.muma.mini.redis.common.HyperLogLog;
import org.muma.mini.redis.common.RedisData;
import org.muma.mini.redis.common.RedisDataType;
import org.muma.mini.redis.protocol.*;
import org.muma.mini.redis.server.RedisContext;
import org.muma.mini.redis.store.StorageEngine;

/**
 * PFADD key element [element ...]
 */
public class PfAddCommand implements RedisCommand {
    @Override
    public RedisMessage execute(StorageEngine storage, RedisArray args, RedisContext context) {
        if (args.elements().length < 3) return errorArgs("pfadd");

        String key = ((BulkString) args.elements()[1]).asString();
        int updated = 0;

        synchronized (storage.getLock(key)) {
            RedisData<?> data = storage.get(key);
            byte[] registers;

            if (data == null) {
                registers = new byte[256]; // M=256
                data = new RedisData<>(RedisDataType.STRING, registers);
                storage.put(key, (RedisData<byte[]>) data);
            } else {
                if (data.getType() != RedisDataType.STRING) return new ErrorMessage("WRONGTYPE");
                registers = data.getValue(byte[].class);
                if (registers.length != 256) return new ErrorMessage("ERR invalid HLL");
            }

            for (int i = 2; i < args.elements().length; i++) {
                byte[] element = ((BulkString) args.elements()[i]).content();
                if (HyperLogLog.add(registers, element)) {
                    updated = 1;
                }
            }

            // 已经是引用修改，HLL 也是 String 类型，无需额外操作
            // 为了 AOF 回写一致性，可以 put 一下
            // storage.put(key, data);
        }
        return new RedisInteger(updated);
    }

    @Override
    public boolean isWrite() {
        return true;
    }
}
