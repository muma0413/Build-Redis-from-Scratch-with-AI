package org.muma.mini.redis.replication;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.muma.mini.redis.protocol.BulkString;
import org.muma.mini.redis.protocol.ErrorMessage;
import org.muma.mini.redis.protocol.RedisMessage;
import org.muma.mini.redis.protocol.SimpleString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Slave 端的 Netty Handler
 * 负责处理 Master 发回的握手响应、RDB 数据流、Command 传播流。
 */
public class RedisSlaveHandler extends SimpleChannelInboundHandler<RedisMessage> {

    private static final Logger log = LoggerFactory.getLogger(RedisSlaveHandler.class);
    private final ReplicationManager manager;

    public RedisSlaveHandler(ReplicationManager manager) {
        this.manager = manager;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, RedisMessage msg) {
        ReplState state = manager.getState();

        // 【新增调试日志】
        log.info("Slave received msg in state {}: {}", state, msg.getClass().getSimpleName());

        // 错误处理
        if (msg instanceof ErrorMessage err) {
            log.error("Master responded error: {}", err.content());
            // 简单处理：报错则断开重连
            ctx.close();
            return;
        }

        switch (state) {
            case RECEIVE_PONG:
                if (isOk(msg) || "PONG".equalsIgnoreCase(content(msg))) {
                    log.info("Master PONG received.");
                    manager.sendReplConfPort();
                }
                break;

            case SEND_PORT: // 等待 Port 的 OK
                if (isOk(msg)) {
                    manager.sendReplConfCapa();
                }
                break;

            case SEND_CAPA: // 等待 Capa 的 OK
                if (isOk(msg)) {
                    manager.sendPsync();
                }
                break;

            case RECEIVE_PSYNC:
                // 这里是关键！Master 会回复 +FULLRESYNC 或 +CONTINUE
                if (msg instanceof SimpleString ss) {
                    String resp = ss.content();
                    if (resp.startsWith("FULLRESYNC")) {
                        log.info("Master request FULLRESYNC: {}", resp);
                        // 解析 RunID 和 Offset
                        String[] parts = resp.split(" ");
                        manager.handleFullResync(parts[1], Long.parseLong(parts[2]));
                    } else if (resp.startsWith("CONTINUE")) {
                        log.info("Master request CONTINUE");
                        manager.handleContinue();
                    }
                }
                break;

            case TRANSFER:
                // 等待 RDB (BulkString)
                log.info("slave receive transfer message: {} {}", msg, msg.getClass().getSimpleName());
                if (msg instanceof BulkString bs) {
                    manager.handleRdbDump(bs.content());
                } else {
                    // 如果收到其他的 (比如 PING? 或者分包错误)，忽略或报错
                    log.warn("Expected RDB (BulkString) in TRANSFER state, but got: {}", msg.getClass().getSimpleName());
                }
                break;

            case CONNECTED:
                // 接收 Master 传播过来的写命令 (SET, DEL...)
                // 转发给 CommandDispatcher 执行
                manager.handlePropagatedCommand(msg);
                break;
        }
    }


    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        log.warn("Connection to master lost.");
        // 通知 Manager
        manager.handleMasterDisconnection();
        super.channelInactive(ctx);
    }

    private boolean isOk(RedisMessage msg) {
        return msg instanceof SimpleString ss && "OK".equalsIgnoreCase(ss.content());
    }

    private String content(RedisMessage msg) {
        if (msg instanceof SimpleString ss) return ss.content();
        return "";
    }
}
