package org.muma.mini.redis.command.impl.hash;

import org.muma.mini.redis.command.RedisCommand;
import org.muma.mini.redis.common.RedisData;
import org.muma.mini.redis.common.RedisDataType;
import org.muma.mini.redis.common.RedisHash;
import org.muma.mini.redis.protocol.*;
import org.muma.mini.redis.server.RedisContext;
import org.muma.mini.redis.store.StorageEngine;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;

/**
 * HINCRBYFLOAT key field increment
 *
 * 为哈希表 key 中的指定字段的浮点数值加上增量 increment 。
 * Time Complexity: O(1)
 */
public class HIncrByFloatCommand implements RedisCommand {
    @Override
    public RedisMessage execute(StorageEngine storage, RedisArray args, RedisContext context) {
        if (args.elements().length != 4) return errorArgs("hincrbyfloat");

        String key = ((BulkString) args.elements()[1]).asString();
        String field = ((BulkString) args.elements()[2]).asString();
        double increment;
        try {
            increment = Double.parseDouble(((BulkString) args.elements()[3]).asString());
            if (Double.isNaN(increment) || Double.isInfinite(increment)) {
                return new ErrorMessage("ERR value is not a valid float");
            }
        } catch (NumberFormatException e) {
            return new ErrorMessage("ERR value is not a valid float");
        }

        synchronized (storage.getLock(key)) {
            RedisData<?> data = storage.get(key);
            RedisHash hash;

            if (data == null) {
                hash = new RedisHash();
                data = new RedisData<>(RedisDataType.HASH, hash);
            } else {
                if (data.getType() != RedisDataType.HASH) {
                    return new ErrorMessage("WRONGTYPE Operation against a key holding the wrong kind of value");
                }
                hash = data.getValue(RedisHash.class);
            }

            // 1. 获取旧值
            byte[] oldBytes = hash.get(field);
            double oldVal = 0.0;
            if (oldBytes != null) {
                try {
                    oldVal = Double.parseDouble(new String(oldBytes, StandardCharsets.UTF_8));
                } catch (NumberFormatException e) {
                    return new ErrorMessage("ERR hash value is not a valid float");
                }
            }

            // 2. 计算新值 (使用 BigDecimal 避免简单的精度丢失？Redis 其实直接用 double)
            // 这里为了简单且符合 Redis 行为，直接用 double 运算
            double newVal = oldVal + increment;

            // 3. 格式化 (去除末尾无用的 0)
            // 比如 5.0 -> 5, 5.50 -> 5.5
            // 简单实现：BigDecimal.stripTrailingZeros() 或者手动判断
            String newValStr = new BigDecimal(String.valueOf(newVal)).stripTrailingZeros().toPlainString();

            // 4. 存回
            hash.put(field, newValStr.getBytes(StandardCharsets.UTF_8));
            storage.put(key, data);

            return new BulkString(newValStr);
        }
    }

    @Override
    public boolean isWrite() {
        return true;
    }
}
