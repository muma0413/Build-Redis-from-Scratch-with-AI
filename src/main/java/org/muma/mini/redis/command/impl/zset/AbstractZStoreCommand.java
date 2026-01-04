package org.muma.mini.redis.command.impl.zset;

import org.muma.mini.redis.command.RedisCommand;
import org.muma.mini.redis.common.RedisData;
import org.muma.mini.redis.common.RedisDataType;
import org.muma.mini.redis.common.RedisZSet;
import org.muma.mini.redis.protocol.*;
import org.muma.mini.redis.server.RedisContext;
import org.muma.mini.redis.store.StorageEngine;

import java.util.ArrayList;
import java.util.List;

/**
 * ZUNIONSTORE / ZINTERSTORE 的通用模板类
 * <p>
 * 【架构设计】
 * 由于这两个命令参数结构完全一致，只是核心计算逻辑不同，
 * 使用模板方法模式 (Template Method Pattern) 消除重复代码。
 * <p>
 * 【锁策略 / Locking Strategy】
 * 这是一个 "多 Key 读 + 单 Key 写" 的原子操作。
 * 在 Redis 单线程模型中，这是天然原子的。
 * 在我们的多线程 Java 模型中，为了保证计算期间源数据不被修改，
 * 最安全的做法是加【全库锁 (Global Lock)】。
 * (注：虽然性能有损耗，但对于聚合类重指令，正确性优先于并发度)
 */
public abstract class AbstractZStoreCommand implements RedisCommand {

    protected static final int AGGREGATE_SUM = 0;
    protected static final int AGGREGATE_MIN = 1;
    protected static final int AGGREGATE_MAX = 2;

    @Override
    public RedisMessage execute(StorageEngine storage,RedisArray args, RedisContext context) {
        RedisMessage[] elements = args.elements();

        // 1. 基础参数校验
        if (elements.length < 4) return errorArgs(getCommandName());

        String destKey = ((BulkString) elements[1]).asString();
        int numKeys;
        try {
            numKeys = Integer.parseInt(((BulkString) elements[2]).asString());
        } catch (NumberFormatException e) {
            return errorInt();
        }

        if (elements.length < 3 + numKeys) return errorArgs(getCommandName());

        // 2. 收集源 Key
        List<String> srcKeys = new ArrayList<>();
        for (int i = 0; i < numKeys; i++) {
            srcKeys.add(((BulkString) elements[3 + i]).asString());
        }

        // 3. 解析可选参数 (WEIGHTS, AGGREGATE)
        double[] weights = new double[numKeys];
        // 默认权重均为 1.0
        for (int i = 0; i < numKeys; i++) weights[i] = 1.0;
        int aggregate = AGGREGATE_SUM;

        int idx = 3 + numKeys;
        while (idx < elements.length) {
            String opt = ((BulkString) elements[idx]).asString().toUpperCase();
            if ("WEIGHTS".equals(opt)) {
                if (idx + numKeys >= elements.length) return new ErrorMessage("ERR syntax error");
                idx++;
                for (int i = 0; i < numKeys; i++) {
                    try {
                        weights[i] = Double.parseDouble(((BulkString) elements[idx + i]).asString());
                    } catch (NumberFormatException e) {
                        return new ErrorMessage("ERR weight value is not a float");
                    }
                }
                idx += numKeys;
            } else if ("AGGREGATE".equals(opt)) {
                if (idx + 1 >= elements.length) return new ErrorMessage("ERR syntax error");
                String type = ((BulkString) elements[idx + 1]).asString().toUpperCase();
                if ("SUM".equals(type)) aggregate = AGGREGATE_SUM;
                else if ("MIN".equals(type)) aggregate = AGGREGATE_MIN;
                else if ("MAX".equals(type)) aggregate = AGGREGATE_MAX;
                else return new ErrorMessage("ERR syntax error");
                idx += 2;
            } else {
                return new ErrorMessage("ERR syntax error");
            }
        }

        long resultSize = 0;

        // 【关键点】全库加锁，防止计算过程中源 Key 被其他线程修改
        synchronized (storage) {
            List<RedisZSet> srcSets = new ArrayList<>();
            for (String k : srcKeys) {
                RedisData<?> d = storage.get(k);
                if (d == null) {
                    srcSets.add(null); // Key 不存在视为 Empty Set
                } else if (d.getType() != RedisDataType.ZSET) {
                    // 如果源 Key 类型不对，Redis 标准行为是报错
                    return new ErrorMessage("WRONGTYPE Operation against a key holding the wrong kind of value");
                } else {
                    srcSets.add(d.getValue(RedisZSet.class));
                }
            }

            // 调用子类实现的具体算法
            RedisZSet destSet = compute(srcSets, weights, aggregate);

            // 结果存入目标 Key
            if (destSet.size() > 0) {
                storage.put(destKey, new RedisData<>(RedisDataType.ZSET, destSet));
            } else {
                // 如果结果集为空，Redis 规范是删除目标 Key
                storage.remove(destKey);
            }
            resultSize = destSet.size();
        }

        return new RedisInteger(resultSize);
    }

    protected abstract String getCommandName();

    /**
     * 核心计算逻辑，由子类实现 (Union 或 Inter)
     */
    protected abstract RedisZSet compute(List<RedisZSet> srcSets, double[] weights, int aggregate);
}
