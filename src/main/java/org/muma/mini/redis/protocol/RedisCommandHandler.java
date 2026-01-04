package org.muma.mini.redis.protocol;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.muma.mini.redis.command.CommandDispatcher;
import org.muma.mini.redis.store.StorageEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.util.Arrays;
import java.util.Locale;
import java.util.stream.Collectors;

public class RedisCommandHandler extends SimpleChannelInboundHandler<RedisMessage> {

    private static final Logger log = LoggerFactory.getLogger(RedisCommandHandler.class);

    // 记录连接的客户端数量（简单计数）
    private static int connectedClients = 0;

    private final CommandDispatcher dispatcher;

    public RedisCommandHandler(StorageEngine storage) {
        this.dispatcher = new CommandDispatcher(storage);
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        connectedClients++;
        log.info("Client connected: {}, total clients: {}", ctx.channel().remoteAddress(), connectedClients);
        super.channelActive(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        connectedClients--;
        log.info("Client disconnected: {}, total clients: {}", ctx.channel().remoteAddress(), connectedClients);
        super.channelInactive(ctx);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, RedisMessage msg) {
        if (msg instanceof RedisArray array) {
            handleCommand(ctx, array);
        } else {
            log.warn("Received non-array message: {}", msg);
            ctx.writeAndFlush(new ErrorMessage("ERR protocol error: expected array"));
        }
    }

    private void handleCommand(ChannelHandlerContext ctx, RedisArray array) {
        RedisMessage[] elements = array.elements();
        if (elements == null || elements.length == 0) return;

        if (!(elements[0] instanceof BulkString cmdNameBulk)) {
            ctx.writeAndFlush(new ErrorMessage("ERR protocol error: command name must be string"));
            return;
        }

        String commandName = cmdNameBulk.asString().toUpperCase(Locale.ROOT);

        // 记录日志，忽略 INFO 和 SCAN 的详细参数以免刷屏，但初次调试可以保留
        if (log.isDebugEnabled() || !commandName.equals("INFO")) {
            String argsLog = Arrays.stream(elements).skip(1).map(this::convertToString).collect(Collectors.joining(", "));
            log.info("Execute Command: {} args=[{}]", commandName, argsLog);
        }

        try {
            RedisMessage response = switch (commandName) {
                case "PING" -> new SimpleString("PONG");
                case "ECHO" -> handleEcho(elements);
                case "QUIT" -> {
                    ctx.close();
                    yield null;
                }
                case "COMMAND" -> dispatcher.dispatch(commandName, array, ctx);
                case "SCAN" -> handleScanMock(elements);

                // --- 新增 INFO 处理 ---
                case "INFO" -> handleInfo(elements);
                // ---------------------

                default -> dispatcher.dispatch(commandName, array, ctx);
            };

            if (response != null) {
                ctx.writeAndFlush(response);
            }
        } catch (Exception e) {
            log.error("Error processing command {}", commandName, e);
            ctx.writeAndFlush(new ErrorMessage("ERR internal error"));
        }
    }

    private RedisMessage handleInfo(RedisMessage[] elements) {
        // ARDM 或 redis-cli 有时会发 "INFO" 有时发 "INFO server" / "INFO all"
        // 简单起见，我们统一返回核心 Server 段信息。

        long uptime = ManagementFactory.getRuntimeMXBean().getUptime() / 1000;
        long pid = ProcessHandle.current().pid(); // JDK 9+ 获取 PID

        // JDK 15+ Text Blocks: 格式化非常清晰
        String info = """
                # Server
                redis_version:6.0.0
                redis_git_sha1:00000000
                redis_git_dirty:0
                redis_build_id:0
                redis_mode:standalone
                os:%s
                arch_bits:64
                multiplexing_api:netty
                process_id:%d
                tcp_port:6379
                uptime_in_seconds:%d
                uptime_in_days:%d
                executable:mini-redis-java
                
                # Clients
                connected_clients:%d
                
                # Memory
                used_memory_human:1.00M
                used_memory_peak_human:1.00M
                
                # Persistence
                loading:0
                
                # Stats
                total_connections_received:%d
                total_commands_processed:0
                
                # Replication
                role:master
                connected_slaves:0
                
                # CPU
                used_cpu_sys:0.0
                used_cpu_user:0.0
                """.formatted(
                System.getProperty("os.name"),
                pid,
                uptime,
                uptime / (3600 * 24),
                connectedClients,
                connectedClients // 简单mock，每次连接算一次
        );

        return new BulkString(info);
    }

    // ... 其他方法 (handleScanMock, handleEcho, convertToString) 保持不变 ...

    private RedisMessage handleScanMock(RedisMessage[] elements) {
        return new RedisArray(new RedisMessage[]{
                new BulkString("0"),
                new RedisArray(new RedisMessage[]{
                        new BulkString("mini:version"),
                        new BulkString("author:root_agent")
                })
        });
    }

    private RedisMessage handleEcho(RedisMessage[] elements) {
        if (elements.length != 2) return new ErrorMessage("ERR wrong number of arguments for 'echo' command");
        return elements[1];
    }

    private String convertToString(RedisMessage msg) {
        if (msg instanceof BulkString b) return b.asString();
        if (msg instanceof SimpleString s) return s.content();
        if (msg instanceof RedisInteger i) return String.valueOf(i.value());
        return "<?>";
    }
}
