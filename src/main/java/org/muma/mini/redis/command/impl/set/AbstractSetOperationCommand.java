package org.muma.mini.redis.command.impl.set;

import org.muma.mini.redis.command.RedisCommand;
import org.muma.mini.redis.common.RedisData;
import org.muma.mini.redis.common.RedisDataType;
import org.muma.mini.redis.common.RedisSet;
import org.muma.mini.redis.protocol.BulkString;
import org.muma.mini.redis.protocol.ErrorMessage;
import org.muma.mini.redis.protocol.RedisArray;
import org.muma.mini.redis.protocol.RedisMessage;
import org.muma.mini.redis.server.RedisContext;
import org.muma.mini.redis.store.StorageEngine;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * SUNION / SINTER / SDIFF 的通用模板
 */
public abstract class AbstractSetOperationCommand implements RedisCommand {

    @Override
    public RedisMessage execute(StorageEngine storage, RedisArray args, RedisContext context) {
        if (args.elements().length < 2) return errorArgs(getCommandName());

        List<String> keys = new ArrayList<>();
        for (int i = 1; i < args.elements().length; i++) {
            keys.add(((BulkString) args.elements()[i]).asString());
        }

        // 使用 Set<ByteBuffer> 来去重和存储结果 (byte[] 的 hashCode 是地址，不能直接用)
        // 为了方便，我们在 Command 层先统一转成 ByteBuffer 进行计算，最后转回 byte[]
        // 或者简单点，我们复用 RedisSet 的 getAll，它返回 List<byte[]>
        // 我们在 compute 里自己处理去重。

        List<RedisSet> sets = new ArrayList<>();

        synchronized (storage) { // 全库锁，保证一致性
            for (String key : keys) {
                RedisData<?> data = storage.get(key);
                if (data == null) {
                    sets.add(null);
                } else if (data.getType() != RedisDataType.SET) {
                    return new ErrorMessage("WRONGTYPE Operation against a key holding the wrong kind of value");
                } else {
                    sets.add(data.getValue(RedisSet.class));
                }
            }
        }

        // 核心计算
        Set<java.nio.ByteBuffer> result = compute(sets);

        // 构建响应
        RedisMessage[] response = new RedisMessage[result.size()];
        int i = 0;
        for (java.nio.ByteBuffer bb : result) {
            byte[] bytes = new byte[bb.remaining()];
            bb.get(bytes);
            response[i++] = new BulkString(bytes);
        }
        return new RedisArray(response);
    }

    protected abstract String getCommandName();

    protected abstract Set<java.nio.ByteBuffer> compute(List<RedisSet> sets);

    // 辅助：将 byte[] 转 ByteBuffer
    protected java.nio.ByteBuffer wrap(byte[] bytes) {
        return java.nio.ByteBuffer.wrap(bytes);
    }
}
