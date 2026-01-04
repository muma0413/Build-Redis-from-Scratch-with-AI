package org.muma.mini.redis.command.impl.key;

import org.muma.mini.redis.command.RedisCommand;
import org.muma.mini.redis.protocol.*;
import org.muma.mini.redis.server.RedisContext;
import org.muma.mini.redis.store.StorageEngine;

public class DelCommand implements RedisCommand {
    @Override
    public RedisMessage execute(StorageEngine storage,RedisArray args, RedisContext context) {
        // 格式: DEL key [key ...]
        if (args.elements().length < 2) {
            return new ErrorMessage("ERR wrong number of arguments for 'del' command");
        }

        int deletedCount = 0;
        RedisMessage[] elements = args.elements();

        for (int i = 1; i < elements.length; i++) {
            String key = ((BulkString) elements[i]).asString();
            if (storage.remove(key)) {
                deletedCount++;
            }
        }

        return new RedisInteger(deletedCount);
    }
}

