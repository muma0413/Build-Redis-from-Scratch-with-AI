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
 * BLPOP key [key ...] timeout
 *
 * 阻塞式左弹出。如果列表为空，连接会被阻塞，直到有数据推入或超时。
 * AOF 策略：不记录 BLPOP 本身，而是记录成功的 LPOP 操作。
 */
public class BLPopCommand implements RedisCommand {

    @Override
    public RedisMessage execute(StorageEngine storage, RedisArray args, RedisContext context) {
        RedisMessage[] elements = args.elements();
        if (elements.length < 3) return errorArgs("blpop");

        List<String> keys = new ArrayList<>();
        // 解析 Key 列表
        for (int i = 1; i < elements.length - 1; i++) {
            keys.add(((BulkString) elements[i]).asString());
        }

        // 解析 Timeout
        long timeout;
        try {
            String timeoutStr = ((BulkString) elements[elements.length - 1]).asString();
            if (timeoutStr == null) return errorArgs("blpop");
            timeout = Long.parseLong(timeoutStr);
        } catch (NumberFormatException e) {
            return new ErrorMessage("ERR timeout is not an integer");
        }

        // 1. 尝试非阻塞 Pop (Try Immediate Pop)
        synchronized (storage) {
            for (String key : keys) {
                RedisData<?> data = storage.get(key);
                if (data != null && data.getType() == RedisDataType.LIST) {
                    RedisList list = data.getValue(RedisList.class);

                    if (list.size() > 0) {
                        // 成功弹出
                        byte[] val = list.lpop();

                        // 维护 Storage 状态
                        if (list.size() == 0) {
                            storage.remove(key);
                        } else {
                            // 显式 put，虽然对象引用没变，但保持语义完整
                            storage.put(key, (RedisData<RedisList>) data);
                        }

                        // 【核心新增】手动传播 AOF
                        // 记录一条等价的 LPOP 命令
                        RedisArray lpopCmd = new RedisArray(new RedisMessage[]{
                                new BulkString("LPOP"),
                                new BulkString(key)
                        });
                        storage.appendAof(lpopCmd);

                        // 立即返回 [key, value]
                        return new RedisArray(new RedisMessage[]{
                                new BulkString(key),
                                new BulkString(val)
                        });
                    }
                }
            }
        }

        // 2. 没数据，进入阻塞模式 (Blocking Mode)
        // 注册到 BlockingManager，等待 LPUSH 唤醒
        // targetKey = null (BLPOP 不需要推入其他列表)
        storage.getBlockingManager().addWait(context.getNettyCtx(), keys, timeout, true, null);

        // 返回 null，指示 Handler 暂停响应，保持连接 open
        return null;
    }

    // isWrite() 默认为 false，因为我们是手动传播的
}
