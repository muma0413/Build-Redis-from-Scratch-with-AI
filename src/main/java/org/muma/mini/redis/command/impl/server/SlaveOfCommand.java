package org.muma.mini.redis.command.impl.server;

import org.muma.mini.redis.command.RedisCommand;
import org.muma.mini.redis.protocol.*;
import org.muma.mini.redis.replication.ReplicationManager;
import org.muma.mini.redis.server.RedisContext;
import org.muma.mini.redis.store.StorageEngine;

/**
 * SLAVEOF host port
 * <p>
 * 异步触发复制流程，立即返回 OK。
 */
public class SlaveOfCommand implements RedisCommand {

    private final ReplicationManager replicationManager;

    public SlaveOfCommand(ReplicationManager replicationManager) {
        this.replicationManager = replicationManager;
    }

    @Override
    public RedisMessage execute(StorageEngine storage, RedisArray args, RedisContext context) {
        if (args.elements().length != 3) return errorArgs("slaveof");

        String host = ((BulkString) args.elements()[1]).asString();
        String portStr = ((BulkString) args.elements()[2]).asString();

        int port = 0;
        if (!"ONE".equalsIgnoreCase(portStr)) { // NO ONE
            try {
                port = Integer.parseInt(portStr);
            } catch (NumberFormatException e) {
                return errorInt();
            }
        }

        // 调用 Manager 启动异步流程
        replicationManager.slaveOf(host, port);

        return new SimpleString("OK");
    }
}
