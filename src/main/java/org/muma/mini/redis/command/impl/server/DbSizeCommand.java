package org.muma.mini.redis.command.impl.server;

import org.muma.mini.redis.command.RedisCommand;
import org.muma.mini.redis.protocol.*;
import org.muma.mini.redis.server.RedisContext;
import org.muma.mini.redis.store.StorageEngine;

/**
 * DBSIZE
 * 返回当前数据库的 key 的数量。
 */
public class DbSizeCommand implements RedisCommand {
    @Override
    public RedisMessage execute(StorageEngine storage, RedisArray args, RedisContext context) {
        if (args.elements().length != 1) return errorArgs("dbsize");

        return new RedisInteger(storage.size());
    }
}
