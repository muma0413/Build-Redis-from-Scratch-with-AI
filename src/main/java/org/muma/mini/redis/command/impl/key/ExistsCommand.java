package org.muma.mini.redis.command.impl.key;

import org.muma.mini.redis.command.RedisCommand;
import org.muma.mini.redis.protocol.*;
import org.muma.mini.redis.server.RedisContext;
import org.muma.mini.redis.store.StorageEngine;

public class ExistsCommand implements RedisCommand {
    @Override
    public RedisMessage execute(StorageEngine storage, RedisArray args, RedisContext context) {
        if (args.elements().length < 2) return errorArgs("exists");

        int count = 0;
        // 遍历所有 Key
        for (int i = 1; i < args.elements().length; i++) {
            String key = ((BulkString) args.elements()[i]).asString();
            // storage.get() 自带惰性删除逻辑，所以如果过期了这里会返回 null
            if (storage.get(key) != null) {
                count++;
            }
        }
        return new RedisInteger(count);
    }
}
