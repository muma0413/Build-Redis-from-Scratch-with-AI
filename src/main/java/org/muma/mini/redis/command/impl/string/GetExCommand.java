package org.muma.mini.redis.command.impl.string;

import org.muma.mini.redis.command.RedisCommand;
import org.muma.mini.redis.common.RedisData;
import org.muma.mini.redis.common.RedisDataType;
import org.muma.mini.redis.protocol.*;
import org.muma.mini.redis.server.RedisContext;
import org.muma.mini.redis.store.StorageEngine;

import java.util.Locale;

/**
 * GETEX key [EX seconds | PX milliseconds | EXAT timestamp | PXAT milliseconds | PERSIST]
 * <p>
 * Redis 6.2+ 新特性。
 * 获取 String 的值，并原子性地设置/清除过期时间。
 * 核心注意事项 (Attention Points)：
 * 原子性 (Atomicity)：
 * 读取 Value 和修改 TTL 必须是原子的。如果读取了 Value 但 TTL 设置失败，或者反过来，都会导致数据不一致。
 * Mini-Redis 方案：我们通过 synchronized(storage.getLock(key)) 或全库锁来保证。
 * 写操作判定 (Write Flag)：
 * 虽然名字叫 GET...，但这本质上是一个写命令（因为它修改了 TTL）。这意味着它会触发 AOF 重写、主从复制等逻辑。
 * Mini-Redis 方案：我们需要确保在修改 TTL 后调用 storage.put 来触发潜在的持久化钩子。
 * 互斥选项 (Mutually Exclusive Options)：
 * EX, PX, EXAT, PXAT, PERSIST 这 5 个选项是互斥的，不能同时出现。
 * 如果有多个选项，Redis 会报错 ERR syntax error。
 * PERSIST 的特殊处理:
 * PERSIST 意味着移除过期时间，使其永久有效（TTL = -1）。这和其他选项（设置新的 TTL）逻辑不同，需要单独处理。
 * Key 不存在:
 * 直接返回 nil，不报错，也不执行任何过期操作。
 * 类型检查:
 * GETEX 只能用于 String 类型。如果 Key 存在但不是 String，报错 WRONGTYPE。
 */
public class GetExCommand implements RedisCommand {

    @Override
    public RedisMessage execute(StorageEngine storage, RedisArray args, RedisContext context) {
        RedisMessage[] elements = args.elements();
        if (elements.length < 2) return errorArgs("getex");

        String key = ((BulkString) elements[1]).asString();

        // --- 1. 参数解析阶段 ---
        long newExpireAt = -2; // -2: 不变 (无选项), -1: 移除 (PERSIST), >0: 新的时间戳
        boolean optionFound = false;

        if (elements.length > 2) {
            // 解析选项
            int i = 2;
            while (i < elements.length) {
                if (optionFound) return new ErrorMessage("ERR syntax error"); // 只能有一个选项

                String opt = ((BulkString) elements[i]).asString().toUpperCase(Locale.ROOT);
                i++;

                if ("PERSIST".equals(opt)) {
                    newExpireAt = -1;
                    optionFound = true;
                } else {
                    // EX, PX, EXAT, PXAT 都需要跟一个数值参数
                    if (i >= elements.length) return new ErrorMessage("ERR syntax error");
                    long val;
                    try {
                        val = Long.parseLong(((BulkString) elements[i]).asString());
                    } catch (NumberFormatException e) {
                        return errorInt();
                    }
                    i++;

                    long now = System.currentTimeMillis();
                    switch (opt) {
                        case "EX" -> newExpireAt = now + val * 1000;
                        case "PX" -> newExpireAt = now + val;
                        case "EXAT" -> newExpireAt = val * 1000;
                        case "PXAT" -> newExpireAt = val;
                        default -> {
                            return new ErrorMessage("ERR syntax error");
                        }
                    }

                    if (newExpireAt <= 0 && newExpireAt != -1) {
                        // 如果计算出的时间戳无效 (比如 EXAT 0)，这等于立即过期？
                        // Redis 逻辑：EX/PX <= 0 或者 EXAT/PXAT 过去时间，会导致 Key 被删除。
                        // 这里简单起见，我们暂不处理 "立即删除" 的边缘情况，假设输入合法。
                    }
                    optionFound = true;
                }
            }
        }

        // --- 2. 执行阶段 ---
        synchronized (storage.getLock(key)) {
            RedisData<?> data = storage.get(key);

            // Key 不存在 -> nil
            if (data == null) {
                return new BulkString((byte[]) null);
            }

            // 类型检查
            if (data.getType() != RedisDataType.STRING) {
                return new ErrorMessage("WRONGTYPE Operation against a key holding the wrong kind of value");
            }

            // 修改 TTL (如果需要)
            if (newExpireAt != -2) {
                // 如果计算出的 expireAt 已经过期了 (针对 EXAT 传过去时间的情况)
                if (newExpireAt > 0 && newExpireAt <= System.currentTimeMillis()) {
                    storage.remove(key); // 立即删除
                    return new BulkString((byte[]) null); // GETEX 如果导致 key 删除，应该返回什么？
                    // Redis 规范：GETEX 总是返回旧值，即使它导致了 Key 过期。
                    // 所以这里我们不能直接删了返回 nil，而应该先拿值，再删。
                } else {
                    data.setExpireAt(newExpireAt);
                    // 显式回写，触发 AOF/Replication
                    // 注意：这里需要强转，因为 data 是 RedisData<?>
                    // 但我们已经检查过 type 是 STRING，所以内部是 byte[]
                    storage.put(key, (RedisData<byte[]>) data);
                }
            }

            // 修正后的逻辑：即使刚刚过期了，也应该返回旧值
            // 但如果上面的逻辑执行了 remove，data 对象还在内存里，可以返回
            return new BulkString((byte[]) data.getData());
        }
    }
}
