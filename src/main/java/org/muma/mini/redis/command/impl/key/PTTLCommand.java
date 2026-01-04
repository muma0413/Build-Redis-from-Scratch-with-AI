package org.muma.mini.redis.command.impl.key;

import org.muma.mini.redis.command.RedisCommand;
import org.muma.mini.redis.common.RedisData;
import org.muma.mini.redis.protocol.*;
import org.muma.mini.redis.server.RedisContext;
import org.muma.mini.redis.store.StorageEngine;

public class PTTLCommand implements RedisCommand {
    @Override
    public RedisMessage execute(StorageEngine storage, RedisArray args, RedisContext context) {
        if (args.elements().length != 2) return errorArgs("pttl");

        String key = ((BulkString) args.elements()[1]).asString();
        RedisData<?> data = storage.get(key);

        if (data == null) {
            return new RedisInteger(-2);
        }

        long expireAt = data.getExpireAt();
        if (expireAt == -1) {
            return new RedisInteger(-1);
        }

        long ttlMs = expireAt - System.currentTimeMillis();
        if (ttlMs < 0) {
            return new RedisInteger(-2);
        }

        return new RedisInteger(ttlMs); // 返回毫秒
    }
}
