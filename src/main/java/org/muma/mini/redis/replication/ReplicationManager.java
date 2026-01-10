package org.muma.mini.redis.replication;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import lombok.Getter;
import org.muma.mini.redis.config.MiniRedisConfig;
import org.muma.mini.redis.protocol.BulkString;
import org.muma.mini.redis.protocol.RedisArray;
import org.muma.mini.redis.protocol.RedisMessage;
import org.muma.mini.redis.protocol.RespDecoder;
import org.muma.mini.redis.protocol.RespEncoder;
import org.muma.mini.redis.server.RedisCoreExecutor;
import org.muma.mini.redis.store.StorageEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * 复制管理器 (Replication Manager)
 * 同时负责 Master 和 Slave 的角色逻辑。
 */
public class ReplicationManager {

    private static final Logger log = LoggerFactory.getLogger(ReplicationManager.class);

    private final MiniRedisConfig config;
    private final StorageEngine storage;
    private final RedisCoreExecutor coreExecutor;

    // --- Getters ---
    @Getter
    private final ReplicationMetadata metadata;
    @Getter
    private volatile ReplState state = ReplState.NONE;

    // --- Slave 角色字段 ---
    private volatile Channel masterChannel;

    // --- Master 角色字段 ---
    // 1. 在线 Slave 列表 (已完成同步，直接转发命令)
    private final List<ChannelHandlerContext> onlineSlaves = new CopyOnWriteArrayList<>();

    // 2. 正在全量同步中的 Slave (Pending)
    // Key: Slave Connection
    // Value: 缓冲区 (在该 Slave 等待 RDB 期间产生的新命令)
    private final Map<ChannelHandlerContext, List<RedisArray>> pendingSlaves = new ConcurrentHashMap<>();

    public ReplicationManager(MiniRedisConfig config, StorageEngine storage, RedisCoreExecutor coreExecutor) {
        this.config = config;
        this.storage = storage;
        this.coreExecutor = coreExecutor;
        this.metadata = new ReplicationMetadata();
    }

    // =========================================================
    // Master 角色逻辑 (Level 2 核心)
    // =========================================================

    /**
     * 添加一个新的 Slave 连接
     * 此时 Slave 刚发来 PSYNC，准备开始同步
     */
    public void addSlave(ChannelHandlerContext ctx) {
        // 先放入 Pending 状态，等待 RDB 传输
        // 使用 synchronized List 或者 Vector 保证线程安全，或者在 core 线程操作
        // 这里 value 是 ArrayList，但在 put 时是原子的
        pendingSlaves.put(ctx, Collections.synchronizedList(new ArrayList<>()));
        log.info("New slave added to pending list: {}", ctx.channel().remoteAddress());
    }

    /**
     * 命令传播 (Propagate)
     * 当主线程执行完写命令后调用
     */
    public void propagate(RedisArray command) {
        // 1. 发送给 Online Slaves
        for (ChannelHandlerContext slave : onlineSlaves) {
            if (slave.channel().isActive()) {
                slave.writeAndFlush(command);
            } else {
                onlineSlaves.remove(slave); // 懒惰清理
            }
        }

        // 2. 缓冲给 Pending Slaves
        // 这里的遍历开销在 Slave 很多时可能较大，Redis 优化是用全局 Backlog
        // Mini-Redis 简化：直接给每个 Pending Slave 存一份
        if (!pendingSlaves.isEmpty()) {
            for (Map.Entry<ChannelHandlerContext, List<RedisArray>> entry : pendingSlaves.entrySet()) {
                entry.getValue().add(command);
            }
        }

        // 3. 更新全局 Offset (暂略，Phase 6)
    }

    /**
     * RDB 发送完成后的回调 (Level 3 预留)
     * 将 Slave 从 Pending 晋升为 Online
     */
    public void promoteSlaveToOnline(ChannelHandlerContext ctx) {
        List<RedisArray> buffer = pendingSlaves.remove(ctx);
        if (buffer != null) {
            log.info("Promoting slave to Online. Replaying {} buffered commands.", buffer.size());

            // 1. 发送缓冲区里的积压命令
            for (RedisArray cmd : buffer) {
                ctx.write(cmd); // write 不 flush
            }
            ctx.flush();

            // 2. 加入 Online 列表
            onlineSlaves.add(ctx);
        }
    }

    // =========================================================
    // Slave 角色逻辑 (Level 1 回顾)
    // =========================================================

    public void slaveOf(String host, int port) {
        if ("NO".equalsIgnoreCase(host) && "ONE".equalsIgnoreCase(String.valueOf(port))) {
            metadata.clearMaster();
            state = ReplState.NONE;
            if (masterChannel != null) masterChannel.close();
            log.info("Turned into a MASTER");
            return;
        }

        metadata.setMaster(host, port);
        state = ReplState.CONNECT;
        log.info("SLAVEOF {}:{} enabled, state: CONNECT", host, port);
        coreExecutor.submit(this::connectToMaster);
    }

    private void connectToMaster() {
        Bootstrap b = new Bootstrap();
        b.group(new NioEventLoopGroup(1))
                .channel(NioSocketChannel.class)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.pipeline()
                                .addLast(new RespDecoder())
                                .addLast(new RespEncoder())
                                .addLast(new RedisSlaveHandler(ReplicationManager.this));
                    }
                });

        b.connect(metadata.getMasterHost(), metadata.getMasterPort())
                .addListener((ChannelFutureListener) future -> {
                    if (future.isSuccess()) {
                        log.info("Connected to master");
                        masterChannel = future.channel();
                        coreExecutor.submit(this::sendPing);
                    } else {
                        log.warn("Failed to connect to master, retrying in 1s...");
                        Thread.sleep(1000);
                        coreExecutor.submit(this::connectToMaster);
                    }
                });
    }

    // --- State Actions ---
    public void sendPing() {
        state = ReplState.RECEIVE_PONG;
        writeToMaster(new RedisArray(new RedisMessage[]{ new BulkString("PING") }));
    }
    public void sendReplConfPort() {
        state = ReplState.SEND_PORT;
        writeToMaster(new RedisArray(new RedisMessage[]{
                new BulkString("REPLCONF"), new BulkString("listening-port"), new BulkString(String.valueOf(config.getPort()))
        }));
    }
    public void sendReplConfCapa() {
        state = ReplState.SEND_CAPA;
        writeToMaster(new RedisArray(new RedisMessage[]{
                new BulkString("REPLCONF"), new BulkString("capa"), new BulkString("psync2")
        }));
    }
    public void sendPsync() {
        state = ReplState.RECEIVE_PSYNC;
        writeToMaster(new RedisArray(new RedisMessage[]{
                new BulkString("PSYNC"), new BulkString("?"), new BulkString("-1")
        }));
    }

    // --- Callbacks for Handler ---

    public void handleFullResync(String runId, long offset) {
        log.info("Full resync triggered. Master RunID: {}, Offset: {}", runId, offset);
        state = ReplState.TRANSFER;
        metadata.setCachedMasterRunId(runId);
        // 接下来的数据流是 RDB，RedisSlaveHandler 需要切换解码器
    }

    public void handleContinue() {
        log.info("Partial sync accepted.");
        state = ReplState.CONNECTED;
    }

    public void handlePropagatedCommand(RedisMessage msg) {
        // Slave 接收到 Master 的命令，直接执行
        // 注意：这里需要拿到 CommandDispatcher 实例
        // 最好通过构造函数注入，或者在 ServerContext 里协调
        // 暂时假设我们能拿到 dispatch (需要在构造函数加参数)
    }

    private void writeToMaster(RedisMessage msg) {
        if (masterChannel != null && masterChannel.isActive()) {
            masterChannel.writeAndFlush(msg);
        }
    }

    public void sendRdbToSlave(ChannelHandlerContext slave, File rdbFile) {
        // TODO: Level 3 实现零拷贝发送
        log.info("RDB generated, ready to send to slave: {}", rdbFile.getName());
    }

}
