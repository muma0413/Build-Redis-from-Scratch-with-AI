package org.muma.mini.redis.command.impl.list;

import org.muma.mini.redis.command.RedisCommand;
import org.muma.mini.redis.common.RedisData;
import org.muma.mini.redis.common.RedisDataType;
import org.muma.mini.redis.common.RedisList;
import org.muma.mini.redis.protocol.BulkString;
import org.muma.mini.redis.protocol.ErrorMessage;
import org.muma.mini.redis.protocol.RedisArray;
import org.muma.mini.redis.protocol.RedisMessage;
import org.muma.mini.redis.server.RedisContext;
import org.muma.mini.redis.store.StorageEngine;

import java.util.ArrayList;
import java.util.List;

/**
 * BRPOP key [key ...] timeout
 */
public class BRPopCommand implements RedisCommand {
    @Override
    public RedisMessage execute(StorageEngine storage, RedisArray args, RedisContext context) {
        RedisMessage[] elements = args.elements();
        if (elements.length < 3) return errorArgs("brpop");

        List<String> keys = new ArrayList<>();
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

        // 1. 尝试非阻塞 RPOP
        synchronized (storage) {
            for (String key : keys) {
                RedisData<?> data = storage.get(key);
                if (data != null && data.getType() == RedisDataType.LIST) {
                    RedisList list = data.getValue(RedisList.class);
                    if (list.size() > 0) {
                        byte[] val = list.rpop();
                        if (list.size() == 0) storage.remove(key);

                        // 显式回写 (为了触发可能的通知/持久化钩子)
                        storage.put(key, data);

                        return new RedisArray(new RedisMessage[]{
                                new BulkString(key),
                                new BulkString(val)
                        });
                    }
                }
            }
        }

        // 2. 阻塞
        storage.getBlockingManager().addWait(context.getNettyCtx(), keys, timeout, false, null); // false = RPOP

        return null;
    }
}
