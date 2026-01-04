package org.muma.mini.redis.command.impl.list;

import io.netty.channel.ChannelHandlerContext; // 这里的 execute 需要能拿到 ctx
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
 */
public class BLPopCommand implements RedisCommand {

    @Override
    public RedisMessage execute(StorageEngine storage, RedisArray args, RedisContext ctx) {
        RedisMessage[] elements = args.elements();
        if (elements.length < 3) return errorArgs("blpop");

        List<String> keys = new ArrayList<>();
        // 最后一个参数是 timeout
        for (int i = 1; i < elements.length - 1; i++) {
            keys.add(((BulkString) elements[i]).asString());
        }

        long timeout;
        try {
            assert ((BulkString) elements[elements.length - 1]).asString() != null;
            timeout = Long.parseLong(((BulkString) elements[elements.length - 1]).asString());
        } catch (NumberFormatException e) {
            return new ErrorMessage("ERR timeout is not an integer");
        }

        // 1. 尝试非阻塞 Pop
        synchronized (storage) {
            for (String key : keys) {
                RedisData<?> data = storage.get(key);
                if (data != null && data.getType() == RedisDataType.LIST) {
                    RedisList list = data.getValue(RedisList.class);
                    if (list.size() > 0) {
                        byte[] val = list.lpop();
                        if (list.size() == 0) storage.remove(key);

                        // 立即返回: [key, value]
                        return new RedisArray(new RedisMessage[]{
                                new BulkString(key),
                                new BulkString(val)
                        });
                    }
                }
            }
        }

        // 2. 没数据，进入阻塞模式
        storage.getBlockingManager().addWait(ctx.getNettyCtx(), keys, timeout, true, null); // true for Left

        // 返回 null 表示 "暂不响应"，Netty Handler 收到 null 不会 write 任何东西
        // 从而保持连接 open 等待异步唤醒
        return null;
    }
}
