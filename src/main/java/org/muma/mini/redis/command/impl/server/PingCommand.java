package org.muma.mini.redis.command.impl.server;

import org.muma.mini.redis.command.RedisCommand;
import org.muma.mini.redis.protocol.RedisArray;
import org.muma.mini.redis.protocol.RedisMessage;
import org.muma.mini.redis.protocol.SimpleString;
import org.muma.mini.redis.server.RedisContext;
import org.muma.mini.redis.store.StorageEngine;

public class PingCommand implements RedisCommand {
    @Override
    public RedisMessage execute(StorageEngine storage, RedisArray args, RedisContext context) {
        // Slave 收到 PING，通常不需要回复 PONG (因为链接是单向的)
        // 或者回复 PONG (如果 Master 需要检测 Slave 活性)
        // 但在 Redis 复制协议中，Master 发给 Slave 的 PING 只是为了维持连接活性和更新 offset。
        // Slave 执行 PING，什么都不做，或者返回 PONG (如果是交互式 Session)。

        // 在 Slave 复制流中，收到 PING 只是表示心跳，不需要回复。
        // 但 Dispatcher 需要返回一个 Message 给 Handler (虽然 Handler 可能不写回)。
        // 简单返回 "PONG" 即可。
        return new SimpleString("PONG");
    }
}
