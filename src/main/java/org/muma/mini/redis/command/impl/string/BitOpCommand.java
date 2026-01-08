package org.muma.mini.redis.command.impl.string;

import org.muma.mini.redis.command.RedisCommand;
import org.muma.mini.redis.common.RedisData;
import org.muma.mini.redis.common.RedisDataType;
import org.muma.mini.redis.protocol.*;
import org.muma.mini.redis.server.RedisContext;
import org.muma.mini.redis.store.StorageEngine;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * BITOP operation destkey key [key ...]
 *
 * operation: AND, OR, XOR, NOT
 * Time Complexity: O(N)
 */
public class BitOpCommand implements RedisCommand {
    @Override
    public RedisMessage execute(StorageEngine storage, RedisArray args, RedisContext context) {
        if (args.elements().length < 4) return errorArgs("bitop");

        String op = ((BulkString) args.elements()[1]).asString().toUpperCase(Locale.ROOT);
        String destKey = ((BulkString) args.elements()[2]).asString();

        List<String> srcKeys = new ArrayList<>();
        for (int i = 3; i < args.elements().length; i++) {
            srcKeys.add(((BulkString) args.elements()[i]).asString());
        }

        // NOT 只能有一个源
        if ("NOT".equals(op) && srcKeys.size() != 1) {
            return new ErrorMessage("ERR BITOP NOT must be called with a single source key.");
        }

        // 必须全库锁或者批量锁，因为涉及多个 Key 读取和新 Key 写入
        synchronized (storage) {
            List<byte[]> srcBytes = new ArrayList<>();
            int maxLength = 0;

            for (String key : srcKeys) {
                RedisData<?> data = storage.get(key);
                if (data == null) {
                    // 不存在的 Key 视为全 0，长度 0
                    srcBytes.add(new byte[0]);
                } else if (data.getType() != RedisDataType.STRING) {
                    // Redis 碰到类型错误会直接报错吗？
                    // "WRONGTYPE Operation against a key holding the wrong kind of value"
                    // 这里我们为了稳健，直接报错
                    return new ErrorMessage("WRONGTYPE Operation against a key holding the wrong kind of value");
                } else {
                    byte[] b = data.getValue(byte[].class);
                    srcBytes.add(b);
                    if (b.length > maxLength) maxLength = b.length;
                }
            }

            byte[] res = new byte[maxLength];

            if ("NOT".equals(op)) {
                byte[] src = srcBytes.get(0);
                for (int i = 0; i < src.length; i++) {
                    res[i] = (byte) ~src[i];
                }
                // 如果 src 比 maxLength 短 (不可能，因为 maxLength 就是 src.length)，不需要补 0 (补0取反是1，Redis 语义是不处理不存在的部分)
                // 实际上 NOT 操作结果长度等于源长度
            } else {
                // AND, OR, XOR
                // 初始化 res
                if ("AND".equals(op)) {
                    // AND 初始化全 1？不，按位操作逻辑
                    // 只要有一个由短变长补 0，AND 结果就是 0。
                    // 所以 res 只需要拷贝第一个非空数组，然后跟后面的运算即可。
                    // 简单做法：遍历每一个字节索引 i
                    for (int i = 0; i < maxLength; i++) {
                        byte b = getByte(srcBytes.get(0), i);
                        for (int k = 1; k < srcBytes.size(); k++) {
                            b &= getByte(srcBytes.get(k), i);
                        }
                        res[i] = b;
                    }
                } else if ("OR".equals(op)) {
                    for (int i = 0; i < maxLength; i++) {
                        byte b = 0;
                        for (byte[] src : srcBytes) {
                            b |= getByte(src, i);
                        }
                        res[i] = b;
                    }
                } else if ("XOR".equals(op)) {
                    for (int i = 0; i < maxLength; i++) {
                        byte b = getByte(srcBytes.get(0), i);
                        for (int k = 1; k < srcBytes.size(); k++) {
                            b ^= getByte(srcBytes.get(k), i);
                        }
                        res[i] = b;
                    }
                } else {
                    return new ErrorMessage("ERR syntax error");
                }
            }

            // 存入结果
            // 优化：如果结果全 0 且长度很大？Redis 还是会存。
            if (res.length == 0) {
                storage.remove(destKey);
            } else {
                storage.put(destKey, new RedisData<>(RedisDataType.STRING, res));
            }

            return new RedisInteger(res.length);
        }
    }

    private byte getByte(byte[] b, int i) {
        return i < b.length ? b[i] : 0;
    }

    @Override
    public boolean isWrite() {
        return true;
    }
}
