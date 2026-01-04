package org.muma.mini.redis.command.impl.key;

import org.muma.mini.redis.command.RedisCommand;
import org.muma.mini.redis.common.RedisData;
import org.muma.mini.redis.protocol.*;
import org.muma.mini.redis.server.RedisContext;
import org.muma.mini.redis.store.StorageEngine;

public class TTLCommand implements RedisCommand {
    @Override
    public RedisMessage execute(StorageEngine storage, RedisArray args, RedisContext context) {
        if (args.elements().length != 2) return errorArgs("ttl");

        String key = ((BulkString) args.elements()[1]).asString();

        // 注意：这里 storage.get 可能会触发惰性删除
        RedisData<?> data = storage.get(key);

        if (data == null) {
            return new RedisInteger(-2); // Key 不存在 (或已过期被删)
        }

        long expireAt = data.getExpireAt();
        if (expireAt == -1) {
            return new RedisInteger(-1); // 存在但无过期时间
        }

        long ttlMs = expireAt - System.currentTimeMillis();
        if (ttlMs < 0) {
            // 理论上 storage.get 应该已经删了，但为了保险
            return new RedisInteger(-2);
        }

        return new RedisInteger(ttlMs / 1000); // 返回秒
    }
}
