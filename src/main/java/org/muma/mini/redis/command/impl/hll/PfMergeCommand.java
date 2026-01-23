package org.muma.mini.redis.command.impl.hll;

import org.muma.mini.redis.command.RedisCommand;
import org.muma.mini.redis.common.HyperLogLog;
import org.muma.mini.redis.common.RedisData;
import org.muma.mini.redis.common.RedisDataType;
import org.muma.mini.redis.protocol.*;
import org.muma.mini.redis.server.RedisContext;
import org.muma.mini.redis.store.StorageEngine;

/**
 * PFMERGE destkey sourcekey [sourcekey ...]
 * <p>
 * 将多个 HyperLogLog 合并为一个。
 * 结果存储在 destkey 中。
 */
public class PfMergeCommand implements RedisCommand {
    @Override
    public RedisMessage execute(StorageEngine storage, RedisArray args, RedisContext context) {
        if (args.elements().length < 3) return errorArgs("pfmerge");

        String destKey = ((BulkString) args.elements()[1]).asString();

        // 目标寄存器 (初始全0)
        byte[] destRegisters = new byte[256];

        // 必须全库锁，涉及多 Key 读取和写入
        synchronized (storage) {
            // 1. 如果目标 Key 已存在，先读出来作为基准
            RedisData<?> destData = storage.get(destKey);
            if (destData != null) {
                if (destData.getType() != RedisDataType.STRING) {
                    return new ErrorMessage("WRONGTYPE Operation against a key holding the wrong kind of value");
                }
                byte[] existing = destData.getValue(byte[].class);
                if (existing.length != 256) return new ErrorMessage("ERR invalid HLL");

                // 复制一份，避免直接修改原数组 (虽然 HLL 是幂等的，但为了事务隔离性)
                System.arraycopy(existing, 0, destRegisters, 0, 256);
            }

            // 2. 遍历源 Key 并合并
            for (int i = 2; i < args.elements().length; i++) {
                String srcKey = ((BulkString) args.elements()[i]).asString();
                RedisData<?> srcData = storage.get(srcKey);

                if (srcData != null) {
                    if (srcData.getType() != RedisDataType.STRING) {
                        return new ErrorMessage("WRONGTYPE Operation against a key holding the wrong kind of value");
                    }
                    byte[] srcRegisters = srcData.getValue(byte[].class);
                    if (srcRegisters.length != 256) return new ErrorMessage("ERR invalid HLL");

                    // 核心合并逻辑: max(reg[i], src[i])
                    HyperLogLog.merge(destRegisters, srcRegisters);
                }
            }

            // 3. 存入目标 Key
            RedisData<byte[]> newDestData = new RedisData<>(RedisDataType.STRING, destRegisters);
            // 继承原有 TTL (Redis 行为：PFMERGE 好像会清除 TTL？不，通常是视为新 Key)
            // 简单起见，不继承

            storage.put(destKey, newDestData);
        }

        return new SimpleString("OK");
    }

    @Override
    public boolean isWrite() {
        return true;
    }
}
