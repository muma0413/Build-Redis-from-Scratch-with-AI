package org.muma.mini.redis.command.impl.replication;

import org.muma.mini.redis.command.RedisCommand;
import org.muma.mini.redis.protocol.*;
import org.muma.mini.redis.server.RedisContext;
import org.muma.mini.redis.store.StorageEngine;

/**
 * REPLCONF <option> <value> ...
 * 用于主从握手阶段交换信息，或者心跳 ACK。
 */
public class ReplConfCommand implements RedisCommand {
    @Override
    public RedisMessage execute(StorageEngine storage, RedisArray args, RedisContext context) {
        // 简单实现：无论 Slave 发什么配置，Master 都说 OK
        // 未来可以在这里记录 Slave 的 listening-port 或处理 ACK 偏移量
        return new SimpleString("OK");
    }
}
