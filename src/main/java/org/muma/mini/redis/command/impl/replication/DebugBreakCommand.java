package org.muma.mini.redis.command.impl.replication;

import org.muma.mini.redis.command.RedisCommand;
import org.muma.mini.redis.protocol.*;
import org.muma.mini.redis.replication.ReplicationManager;
import org.muma.mini.redis.server.RedisContext;
import org.muma.mini.redis.store.StorageEngine;

/**
 * DEBUG REPL_BREAK
 * 强制断开与 Master 的连接，模拟网络故障，测试增量同步。
 */
public class DebugBreakCommand implements RedisCommand {
    private final ReplicationManager manager;

    public DebugBreakCommand(ReplicationManager manager) {
        this.manager = manager;
    }

    @Override
    public RedisMessage execute(StorageEngine storage, RedisArray args, RedisContext context) {
        if (args.elements().length < 2) return errorArgs("debug");

        String sub = ((BulkString) args.elements()[1]).asString();
        if ("REPL_BREAK".equalsIgnoreCase(sub)) {
            // 调用 Manager 的测试方法
            manager.debugSimulateDisconnect();
            return new SimpleString("OK");
        }
        return new ErrorMessage("ERR unknown debug subcommand");
    }
}