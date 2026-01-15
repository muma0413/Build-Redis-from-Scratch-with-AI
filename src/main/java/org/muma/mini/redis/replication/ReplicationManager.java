package org.muma.mini.redis.replication;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import lombok.Getter;
import lombok.Setter;
import org.muma.mini.redis.command.CommandDispatcher;
import org.muma.mini.redis.config.MiniRedisConfig;
import org.muma.mini.redis.protocol.*;
import org.muma.mini.redis.rdb.RdbLoader;
import org.muma.mini.redis.server.RedisCoreExecutor;
import org.muma.mini.redis.store.StorageEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

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

    @Setter
    private CommandDispatcher dispatcher;

    // 2. 正在全量同步中的 Slave (Pending)
    // Key: Slave Connection
    // Value: 缓冲区 (在该 Slave 等待 RDB 期间产生的新命令)
    private final Map<ChannelHandlerContext, List<RedisArray>> pendingSlaves = new ConcurrentHashMap<>();


    // 【新增】Slave 定时发 ACK 任务
    private ScheduledFuture<?> ackTask;

    // 【新增】Master 定时发 PING 任务
    private ScheduledFuture<?> pingTask;

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

        // 【新增】确保 Master 心跳任务启动
        startMasterPingTask(ctx.channel().eventLoop());
    }

    /**
     * 命令传播 (Propagate)
     * 当主线程执行完写命令后调用
     */
    public void propagate(RedisArray command) {

        // 【防死循环】如果自己是 Slave (处于 CONNECT/CONNECTED 等状态)，绝对不传播！
        // 只有 Master (State == NONE) 才传播。
        if (state != ReplState.NONE) {
            return;
        }
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

            // 【新增】停止心跳
            stopSlaveAckTask();
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
        writeToMaster(new RedisArray(new RedisMessage[]{new BulkString("PING")}));
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
        // 【新增】
        transitionToConnected();
    }

    public void handlePropagatedCommand(RedisMessage msg) {
        // Slave 接收到 Master 的命令，直接执行
        // 注意：这里需要拿到 CommandDispatcher 实例
        // 最好通过构造函数注入，或者在 ServerContext 里协调
        // 暂时假设我们能拿到 dispatch (需要在构造函数加参数)
        log.info("handlePropagatedCommand {}", msg.toString());

        if (msg instanceof RedisArray command) {
            try {
                // 收到命令，直接执行
                // 这里的 ctx 传 null？还是传 masterChannel？
                // 既然是 Master 发来的，不需要回复（Slave 对 Master 是只读的）。
                // 所以传 null 是安全的，或者传一个 Dummy Context。

                // 注意：这里需要 CommandDispatcher。
                // 我们在 ReplicationManager 里没有 Dispatcher 的引用。
                // 方案：在 ServerContext 里注入？或者通过构造函数传进来？
                // 之前的构造函数只有 storage, config, executor。
                // 建议：构造函数增加 CommandDispatcher 参数。

                dispatcher.dispatch(command, null);

            } catch (Exception e) {
                log.error("Failed to execute propagated command", e);
            }
        }
    }

    private void writeToMaster(RedisMessage msg) {
        if (masterChannel != null && masterChannel.isActive()) {
            masterChannel.writeAndFlush(msg);
        }
    }

    public void sendRdbToSlave(ChannelHandlerContext slaveCtx, File rdbFile) {
        log.info("RDB generated, ready to send to slave: {}", rdbFile.getName());

        if (!slaveCtx.channel().isActive()) {
            pendingSlaves.remove(slaveCtx);
            return;
        }

        long length = rdbFile.length();
        log.info("Sending RDB to slave: {} ({} bytes)", slaveCtx.channel().remoteAddress(), length);

        try {
            // 【修改】不使用 FileRegion，直接读入内存发送
            // 避免 FileRegion 在某些环境下 (如 Docker/Windows) 的坑
            // 且对于小文件，这样更稳
            byte[] bytes = Files.readAllBytes(rdbFile.toPath());

            // 构造 ByteBuf: Header + Body + 【CRLF】
            ByteBuf composite = Unpooled.buffer((int) length + 22);

            String header = "$" + length + "\r\n";
            composite.writeBytes(header.getBytes(StandardCharsets.UTF_8));

            composite.writeBytes(bytes);

            // 【关键 Fix】补上 \r\n，让 RespDecoder 以为这是一个普通的 BulkString
            composite.writeBytes(new byte[]{'\r', '\n'});

            // 发送
            slaveCtx.writeAndFlush(composite).addListener((ChannelFutureListener) future -> {
                if (future.isSuccess()) {
                    log.info("RDB sent to slave successfully.");
                    promoteSlaveToOnline(slaveCtx);
                } else {
                    log.error("Failed to send RDB to slave", future.cause());
                    pendingSlaves.remove(slaveCtx);
                    slaveCtx.close();
                }
            });

        } catch (IOException e) {
            log.error("Failed to read RDB file", e);
            pendingSlaves.remove(slaveCtx);
            slaveCtx.close();
        }
    }

    // --- Master 心跳逻辑 ---
    private void startMasterPingTask(EventLoop loop) {
        if (pingTask == null) {
            // 每 10s 发送一次 PING
            pingTask = loop.scheduleAtFixedRate(this::sendPingToSlaves, 10, 10, TimeUnit.SECONDS);
        }
    }

    private void sendPingToSlaves() {
        if (onlineSlaves.isEmpty()) return;

        log.info("Master sending PING to {} slaves...", onlineSlaves.size());
        RedisArray ping = new RedisArray(new RedisMessage[]{new BulkString("PING")});
        for (ChannelHandlerContext slave : onlineSlaves) {
            if (slave.channel().isActive()) {
                slave.writeAndFlush(ping);
            }
        }
    }

    // --- Slave 状态切换与 ACK ---
    private void transitionToConnected() {
        state = ReplState.CONNECTED;
        startSlaveAckTask();
    }

    private void startSlaveAckTask() {
        if (masterChannel != null && masterChannel.eventLoop() != null) {
            ackTask = masterChannel.eventLoop().scheduleAtFixedRate(this::sendAck, 1, 1, TimeUnit.SECONDS);
        }
    }

    private void stopSlaveAckTask() {
        if (ackTask != null) {
            ackTask.cancel(false);
            ackTask = null;
        }
    }

    private void sendAck() {
        if (state != ReplState.CONNECTED) return;

        long offset = metadata.getReplOffset();
        // 【新增日志】
        log.info("Slave sending ACK, offset: {}", offset);

        writeToMaster(new RedisArray(new RedisMessage[]{
                new BulkString("REPLCONF"),
                new BulkString("ACK"),
                new BulkString(String.valueOf(offset))
        }));
    }


    public void handleMasterDisconnection() {
        // 如果我们本来就是 Slave 状态 (CONNECT/CONNECTED...)
        if (state != ReplState.NONE) {
            log.info("Reconnecting to master in 1s...");
            state = ReplState.CONNECT; // 重置状态
            masterChannel = null;

            // 延时重连
            // 注意：这里需要 coreExecutor 支持 schedule，或者用 Thread.sleep
            // 简单起见，开个线程或者用 scheduler (如果有)
            new Thread(() -> {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                }
                coreExecutor.submit(this::connectToMaster);
            }).start();
        }
    }

    public void handleRdbDump(byte[] rdbData) {
        log.info("Received RDB dump, size: {} bytes", rdbData.length);

        // 【修改】使用配置的工作目录，而不是 AOF 目录
        // 这样可以确保它生成在 ./target/slave-data/temp-replication.rdb
        File dumpFile = new File(config.getWorkingDir(), "temp-replication.rdb");

        // 确保目录存在
        if (!dumpFile.getParentFile().exists()) {
            dumpFile.getParentFile().mkdirs();
        }

        try {
            Files.write(dumpFile.toPath(), rdbData); // 简单写入

            // 2. 清空当前数据库
            storage.flush();

            // 3. 加载 RDB
            new RdbLoader(storage).load(dumpFile);

            log.info("RDB loaded successfully. Replication synced.");
            // 【新增】转为连接状态并启动心跳
            transitionToConnected();

        } catch (IOException e) {
            log.error("Failed to save/load RDB dump", e);
            if (masterChannel != null) masterChannel.close();
        }
    }

}
