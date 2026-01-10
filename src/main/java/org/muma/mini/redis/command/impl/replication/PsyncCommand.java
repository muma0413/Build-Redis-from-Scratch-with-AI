package org.muma.mini.redis.command.impl.replication;

import io.netty.channel.ChannelHandlerContext;
import org.muma.mini.redis.command.RedisCommand;
import org.muma.mini.redis.protocol.*;
import org.muma.mini.redis.replication.ReplicationManager;
import org.muma.mini.redis.rdb.RdbManager; // 需要注入这个
import org.muma.mini.redis.server.RedisContext;
import org.muma.mini.redis.store.StorageEngine;

import java.io.File;

public class PsyncCommand implements RedisCommand {

    private final ReplicationManager replManager;
    private final RdbManager rdbManager;

    public PsyncCommand(ReplicationManager replManager, RdbManager rdbManager) {
        this.replManager = replManager;
        this.rdbManager = rdbManager;
    }

    @Override
    public RedisMessage execute(StorageEngine storage, RedisArray args, RedisContext context) {
        // PSYNC <runid> <offset>
        // Mini-Redis 暂不支持增量，全部 Full Resync

        String myRunId = replManager.getMetadata().getMyRunId();
        long myOffset = replManager.getMetadata().getReplOffset();

        ChannelHandlerContext slaveCtx = context.getNettyCtx();

        // 1. 注册 Slave 到 Pending 列表 (开始缓存新命令)
        replManager.addSlave(slaveCtx);

        // 2. 触发 BGSAVE
        // 回调逻辑：当 RDB 生成后，发送给 Slave
        rdbManager.triggerBgsave(file -> {
            // 这是在 BGSAVE 线程回调的
            // 3. 发送 RDB 文件 (Level 3 实现)
            replManager.sendRdbToSlave(slaveCtx, file);
        });

        // 4. 立即返回 +FULLRESYNC
        return new SimpleString("FULLRESYNC " + myRunId + " " + myOffset);
    }
}
