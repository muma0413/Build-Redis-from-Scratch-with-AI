package org.muma.mini.redis.command.impl.set;

import org.muma.mini.redis.command.RedisCommand;
import org.muma.mini.redis.common.RedisData;
import org.muma.mini.redis.common.RedisDataType;
import org.muma.mini.redis.common.RedisSet;
import org.muma.mini.redis.protocol.*;
import org.muma.mini.redis.server.RedisContext;
import org.muma.mini.redis.store.StorageEngine;

import java.util.ArrayList;
import java.util.List;

/**
 * SINTERCARD numkeys key [key ...] [LIMIT limit]
 * <p>
 * Redis 7.0 新特性。
 * 计算交集的基数 (Cardinality)，支持 Limit 提前截断。
 * <p>
 * 【时间复杂度】
 * O(N * M)
 * N 是最小集合的大小，M 是集合总数。
 * 如果设置了 LIMIT，最坏情况是 O(Limit * M)，通常极快。
 */
public class SInterCardCommand implements RedisCommand {

    @Override
    public RedisMessage execute(StorageEngine storage, RedisArray args, RedisContext context) {
        RedisMessage[] elements = args.elements();

        // 1. 参数解析：numkeys
        if (elements.length < 3) return errorArgs("sintercard");
        int numKeys;
        try {
            numKeys = Integer.parseInt(((BulkString) elements[1]).asString());
        } catch (NumberFormatException e) {
            return errorInt();
        }

        if (elements.length < 2 + numKeys) return errorArgs("sintercard");

        // 2. 收集 Keys
        List<String> keys = new ArrayList<>();
        for (int i = 0; i < numKeys; i++) {
            keys.add(((BulkString) elements[2 + i]).asString());
        }

        // 3. 解析可选参数 LIMIT
        int limit = 0; // 0 表示无限制
        if (elements.length > 2 + numKeys) {
            String opt = ((BulkString) elements[2 + numKeys]).asString().toUpperCase();
            if ("LIMIT".equals(opt)) {
                if (elements.length > 3 + numKeys) {
                    try {
                        limit = Integer.parseInt(((BulkString) elements[3 + numKeys]).asString());
                        if (limit < 0) limit = 0; // Redis 规范: 负数视为 0 (无限制)
                    } catch (NumberFormatException e) {
                        return errorInt();
                    }
                } else {
                    return new ErrorMessage("ERR syntax error");
                }
            } else {
                return new ErrorMessage("ERR syntax error");
            }
        }

        // 4. 核心计算 (全库锁)
        long intersectCount = 0;

        synchronized (storage) {
            List<RedisSet> sets = new ArrayList<>();

            // 获取所有集合
            for (String k : keys) {
                RedisData<?> d = storage.get(k);
                if (d == null) {
                    // 如果有一个集合不存在，交集必为 0
                    return new RedisInteger(0);
                } else if (d.getType() != RedisDataType.SET) {
                    return new ErrorMessage("WRONGTYPE Operation against a key holding the wrong kind of value");
                } else {
                    RedisSet set = d.getValue(RedisSet.class);
                    // 如果有一个空集合，交集必为 0
                    if (set.size() == 0) return new RedisInteger(0);
                    sets.add(set);
                }
            }

            // 优化策略：找到最小集合作为基准 (Base)
            RedisSet base = sets.get(0);
            int baseIdx = 0;
            for (int i = 1; i < sets.size(); i++) {
                if (sets.get(i).size() < base.size()) {
                    base = sets.get(i);
                    baseIdx = i;
                }
            }

            // 遍历基准集合
            List<byte[]> baseMembers = base.getAll();

            for (byte[] member : baseMembers) {
                boolean allContain = true;
                // 检查其他集合
                for (int i = 0; i < sets.size(); i++) {
                    if (i == baseIdx) continue;
                    if (!sets.get(i).contains(member)) {
                        allContain = false;
                        break;
                    }
                }

                if (allContain) {
                    intersectCount++;
                    // 【核心优化点】达到 Limit 立即停止！
                    if (limit > 0 && intersectCount >= limit) {
                        break;
                    }
                }
            }
        }

        return new RedisInteger(intersectCount);
    }
}
