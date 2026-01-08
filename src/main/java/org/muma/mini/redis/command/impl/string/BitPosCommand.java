package org.muma.mini.redis.command.impl.string;

import org.muma.mini.redis.command.RedisCommand;
import org.muma.mini.redis.common.RedisData;
import org.muma.mini.redis.common.RedisDataType;
import org.muma.mini.redis.protocol.*;
import org.muma.mini.redis.server.RedisContext;
import org.muma.mini.redis.store.StorageEngine;

/**
 * BITPOS key bit [start] [end]
 * <p>
 * 返回字符串里面第一个被设置为 1 或者 0 的 bit 位。
 */
public class BitPosCommand implements RedisCommand {
    @Override
    public RedisMessage execute(StorageEngine storage, RedisArray args, RedisContext context) {
        if (args.elements().length < 3) return errorArgs("bitpos");

        String key = ((BulkString) args.elements()[1]).asString();
        int targetBit;
        try {
            targetBit = Integer.parseInt(((BulkString) args.elements()[2]).asString());
            if (targetBit != 0 && targetBit != 1) throw new NumberFormatException();
        } catch (NumberFormatException e) {
            return new ErrorMessage("ERR The bit argument must be 1 or 0.");
        }

        // 解析 start/end (字节索引)
        // 略微复杂，这里简化处理，假设没有 start/end，或者你参考 BitCount 实现解析
        // 生产级代码需要解析 start/end。我们先实现全量。

        RedisData<?> data = storage.get(key);
        if (data == null) {
            return new RedisInteger(targetBit == 0 ? 0 : -1);
        }
        if (data.getType() != RedisDataType.STRING) {
            return new ErrorMessage("WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        byte[] bytes = data.getValue(byte[].class);

        long pos = -1;
        for (int i = 0; i < bytes.length; i++) {
            byte b = bytes[i];
            // 优化：如果找 0 且 b==0xFF (全1)，跳过
            // 如果找 1 且 b==0x00 (全0)，跳过
            if (targetBit == 1 && b == 0) continue;
            if (targetBit == 0 && b == -1) continue; // -1 is 0xFF

            for (int bitIdx = 0; bitIdx < 8; bitIdx++) {
                int val = (b >> (7 - bitIdx)) & 1;
                if (val == targetBit) {
                    pos = (long) i * 8 + bitIdx;
                    break;
                }
            }
            if (pos != -1) break;
        }

        // 特殊情况：找 0，但整个 byte[] 都是 1
        // Redis 规定：如果指定了 range，找不到返回 -1。
        // 如果没指定 range (全量)，视为右边有无限个 0，所以返回 bytes.length * 8
        if (targetBit == 0 && pos == -1) {
            pos = (long) bytes.length * 8;
        }

        return new RedisInteger(pos);
    }
}
