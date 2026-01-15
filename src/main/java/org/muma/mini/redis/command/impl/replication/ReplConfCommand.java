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
        RedisMessage[] elements = args.elements();
        if (elements.length > 1) {
            String subCmd = ((BulkString) elements[1]).asString();
            if ("ACK".equalsIgnoreCase(subCmd)) {
                // 处理 ACK
                // 可以在这里调用 manager.handleAck(ctx, offset)
                // 但 ReplConfCommand 没持有 manager 引用。
                // 鉴于目前 ACK 只是保活，不影响逻辑，我们可以简单忽略或打印日志。
                return null; // ACK 不需要回复
            }
        }
        return new SimpleString("OK");
    }
}
