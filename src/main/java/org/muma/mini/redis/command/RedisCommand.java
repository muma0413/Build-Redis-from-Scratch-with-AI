package org.muma.mini.redis.command;

import org.muma.mini.redis.protocol.RedisArray;
import org.muma.mini.redis.protocol.RedisMessage;
import org.muma.mini.redis.store.StorageEngine;

public interface RedisCommand {
    // 执行命令，传入存储引擎和参数
    RedisMessage execute(StorageEngine storage, RedisArray args);
}
