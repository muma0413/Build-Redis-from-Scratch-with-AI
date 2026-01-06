package org.muma.mini.redis.command.impl.list;

import org.muma.mini.redis.command.RedisCommand;
import org.muma.mini.redis.common.RedisData;
import org.muma.mini.redis.common.RedisDataType;
import org.muma.mini.redis.common.RedisList;
import org.muma.mini.redis.protocol.*;
import org.muma.mini.redis.server.RedisContext;
import org.muma.mini.redis.store.StorageEngine;

import java.util.ArrayList;
import java.util.List;

/**
 * BRPOP key [key ...] timeout
 *
 * 阻塞式右弹出。
 * AOF 策略：手动传播 RPOP 命令。
 */
public class BRPopCommand implements RedisCommand {

    @Override
    public RedisMessage execute(StorageEngine storage, RedisArray args, RedisContext context) {
        RedisMessage[] elements = args.elements();
        if (elements.length < 3) return errorArgs("brpop");

        List<String> keys = new ArrayList<>();
        // 解析 Key 列表
        for (int i = 1; i < elements.length - 1; i++) {
            keys.add(((BulkString) elements[i]).asString());
        }

        // 解析 Timeout
        long timeout;
        try {
            String timeoutStr = ((BulkString) elements[elements.length - 1]).asString();
            if (timeoutStr == null) return errorArgs("brpop");
            timeout = Long.parseLong(timeoutStr);
        } catch (NumberFormatException e) {
            return new ErrorMessage("ERR timeout is not an integer");
        }

        // 1. 尝试非阻塞 RPOP (Try Immediate RPop)
        synchronized (storage) {
            for (String key : keys) {
                RedisData<?> data = storage.get(key);
                if (data != null && data.getType() == RedisDataType.LIST) {
                    RedisList list = data.getValue(RedisList.class);

                    if (list.size() > 0) {
                        // 成功弹出
                        byte[] val = list.rpop();

                        // 维护 Storage 状态
                        if (list.size() == 0) {
                            storage.remove(key);
                        } else {
                            // 显式回写，保持一致性
                            storage.put(key, (RedisData<RedisList>) data);
                        }

                        // 【核心新增】手动传播 AOF -> RPOP
                        RedisArray rpopCmd = new RedisArray(new RedisMessage[]{
                                new BulkString("RPOP"),
                                new BulkString(key)
                        });
                        storage.appendAof(rpopCmd);

                        // 立即返回 [key, value]
                        return new RedisArray(new RedisMessage[]{
                                new BulkString(key),
                                new BulkString(val)
                        });
                    }
                }
            }
        }

        // 2. 没数据，进入阻塞模式
        // isLeftPop = false (RPOP), targetKey = null
        storage.getBlockingManager().addWait(context.getNettyCtx(), keys, timeout, false, null);

        return null;
    }
}
