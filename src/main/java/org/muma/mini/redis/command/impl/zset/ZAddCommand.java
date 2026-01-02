package org.muma.mini.redis.command.impl.zset;

import org.muma.mini.redis.command.RedisCommand;
import org.muma.mini.redis.common.RedisData;
import org.muma.mini.redis.common.RedisDataType;
import org.muma.mini.redis.common.RedisZSet;
import org.muma.mini.redis.protocol.*;
import org.muma.mini.redis.store.StorageEngine;

/**
 * 命令: ZADD key score member [score member ...]
 * 功能: 将一个或多个 member 元素及其 score 值加入到有序集 key 当中。
 * <p>
 * 【时间复杂度 / Time Complexity】
 * O(K * log(N))
 * - K: 添加元素的数量
 * - N: 有序集合当前的元素数量
 * <p>
 * 【复杂度分析】
 * 1. 对于 SkipList (大数据量): 每次插入需要 O(logN) 的时间来寻找位置和调整指针。
 * 2. 对于 ZipList (小数据量): 虽然插入是 O(N) 的 (数组移动)，但因为 N 被限制在极小值 (默认128)，
 * 所以实际消耗可以视为 O(1) 的常数时间。
 * 3. 总体表现符合 O(K * log(N))。
 */
public class ZAddCommand implements RedisCommand {
    @Override
    public RedisMessage execute(StorageEngine storage, RedisArray args) {
        RedisMessage[] elements = args.elements();

        // 参数校验: ZADD key score member... (长度至少为 4，且减去命令名和Key后必须是偶数)
        if (elements.length < 4 || (elements.length - 2) % 2 != 0) {
            return new ErrorMessage("ERR wrong number of arguments for 'zadd' command");
        }

        String key = ((BulkString) elements[1]).asString();
        int addedCount = 0;

        // 原子性锁
        synchronized (storage.getLock(key)) {
            // 1. 获取数据 (使用通配符泛型)
            RedisData<RedisZSet> data = storage.get(key);
            RedisZSet zset;

            if (data == null) {
                // 新建 ZSet
                zset = new RedisZSet();
                // 构造泛型 RedisData
                data = new RedisData<>(RedisDataType.ZSET, zset);
            } else {
                // 类型检查
                if (data.getType() != RedisDataType.ZSET) {
                    return new ErrorMessage("WRONGTYPE Operation against a key holding the wrong kind of value");
                }
                // 安全获取
                zset = data.getValue(RedisZSet.class);
            }

            // 2. 循环处理每对 score-member
            for (int i = 2; i < elements.length; i += 2) {
                double score;
                try {
                    String scoreStr = ((BulkString) elements[i]).asString();
                    score = Double.parseDouble(scoreStr);
                    if (Double.isNaN(score)) throw new NumberFormatException("NaN");
                } catch (NumberFormatException e) {
                    return new ErrorMessage("ERR value is not a valid float");
                }

                String member = ((BulkString) elements[i + 1]).asString();

                // add 方法内部会自动处理 ZipList -> SkipList 的升级
                addedCount += zset.add(score, member);
            }

            // 3. 回写 Storage (触发潜在的持久化/更新逻辑)
            // 这里我们需要显式转型或重新构造，为了安全建议复用 data 对象引用
            // 如果 data 是新创建的，需要 put；如果是旧的，put 也是为了语义闭环
            storage.put(key, data);
        }

        return new RedisInteger(addedCount);
    }
}
