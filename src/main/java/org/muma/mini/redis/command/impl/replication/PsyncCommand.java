package org.muma.mini.redis.command.impl.replication;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import org.muma.mini.redis.command.RedisCommand;
import org.muma.mini.redis.protocol.*;
import org.muma.mini.redis.replication.ReplicationManager;
import org.muma.mini.redis.rdb.RdbManager;
import org.muma.mini.redis.server.RedisContext;
import org.muma.mini.redis.store.StorageEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PsyncCommand implements RedisCommand {

    private static final Logger log = LoggerFactory.getLogger(PsyncCommand.class);

    private final ReplicationManager replManager;
    private final RdbManager rdbManager;

    public PsyncCommand(ReplicationManager replManager, RdbManager rdbManager) {
        this.replManager = replManager;
        this.rdbManager = rdbManager;
    }

    @Override
    public RedisMessage execute(StorageEngine storage, RedisArray args, RedisContext context) {
        // PSYNC <runid> <offset>
        if (args.elements().length != 3) {
            return new ErrorMessage("ERR wrong number of arguments for 'psync' command");
        }

        String reqRunId = ((BulkString) args.elements()[1]).asString();
        long reqOffset;
        try {
            reqOffset = Long.parseLong(((BulkString) args.elements()[2]).asString());
        } catch (NumberFormatException e) {
            return new ErrorMessage("ERR invalid offset");
        }

        String myRunId = replManager.getMetadata().getMyRunId();
        ChannelHandlerContext slaveCtx = context.getNettyCtx();

        // --- 尝试增量同步 (Partial Resync) ---
        // 条件：RunID 匹配，且 offset 在 Backlog 有效范围内
        if (myRunId.equals(reqRunId) && replManager.getBacklog().isValidOffset(reqOffset)) {

            log.info(">>> [PSYNC HIT] Partial resync accepted! Slave request: runid={}, offset={}", reqRunId, reqOffset);
            log.info("Partial resync accepted. Slave offset: {}", reqOffset);

            // 1. 获取增量数据
            byte[] delta = replManager.getBacklog().getBytesFrom(reqOffset);

            // 2. 将 Slave 直接加入 Online 列表 (不需要等待 RDB)
            replManager.addSlaveToOnline(slaveCtx);

            // 3. 发送 +CONTINUE
            slaveCtx.write(new SimpleString("CONTINUE"));

            // 4. 发送增量数据 (raw bytes)
            if (delta != null && delta.length > 0) {
                slaveCtx.writeAndFlush(Unpooled.wrappedBuffer(delta));
            } else {
                slaveCtx.flush();
            }

            // PSYNC 协议比较特殊，如果是 CONTINUE，Command 自身不返回数据，
            // 而是通过上面的 write 直接写回。
            // 为了防止 RedisCommandHandler 再写一次，我们可以返回 null。
            return null;
        } else {
            log.warn(">>> [PSYNC MISS] Full resync required. Slave: {}, Master: {}. Offset Valid? {}",
                    reqRunId, myRunId, replManager.getBacklog().isValidOffset(reqOffset));
        }

        // --- 降级为全量同步 (Full Resync) ---
        log.info("Full resync required. ReqRunId: {}, ReqOffset: {}", reqRunId, reqOffset);

        long myOffset = replManager.getMetadata().getReplOffset();

        // 1. 注册 Slave 到 Pending 列表
        replManager.addSlave(slaveCtx);

        // 2. 触发 BGSAVE
        rdbManager.triggerBgSave(file -> {
            replManager.sendRdbToSlave(slaveCtx, file);
        });

        // 3. 返回 +FULLRESYNC
        return new SimpleString("FULLRESYNC " + myRunId + " " + myOffset);
    }
}
