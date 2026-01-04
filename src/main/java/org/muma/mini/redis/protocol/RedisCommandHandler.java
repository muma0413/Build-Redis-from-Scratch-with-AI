package org.muma.mini.redis.protocol;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.muma.mini.redis.command.CommandDispatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.util.Arrays;
import java.util.Locale;
import java.util.stream.Collectors;

public class RedisCommandHandler extends SimpleChannelInboundHandler<RedisMessage> {

    private static final Logger log = LoggerFactory.getLogger(RedisCommandHandler.class);

    // 记录连接的客户端数量（简单计数）
    // 注意：这个静态变量在多线程环境下可能不准，建议用 AtomicInteger
    private static int connectedClients = 0;

    // 【修改点 1】持有单例 Dispatcher
    private final CommandDispatcher dispatcher;

    // 【修改点 2】构造函数接收单例 Dispatcher
    public RedisCommandHandler(CommandDispatcher dispatcher) {
        this.dispatcher = dispatcher;
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

        // 记录日志
        if (log.isDebugEnabled() || !commandName.equals("INFO")) {
            String argsLog = Arrays.stream(elements).skip(1).map(this::convertToString).collect(Collectors.joining(", "));
            log.info("Execute Command: {} args=[{}]", commandName, argsLog);
        }

        try {
            RedisMessage response = switch (commandName) {
                // 特殊连接管理命令，可以在这里拦截，也可以全部下沉到 Dispatcher
                case "PING" -> new SimpleString("PONG");
                case "ECHO" -> handleEcho(elements);
                case "QUIT" -> {
                    ctx.close();
                    yield null;
                }

                // 元数据 Mock 命令 (你可以选择保留在这里，或者写成 Command 类注册进 Dispatcher)
                case "COMMAND" -> new SimpleString("OK"); // 简单 Mock，防止客户端报错
                case "SCAN" -> handleScanMock(elements);
                case "INFO" -> handleInfo(elements);

                // 【核心修改】其余所有数据命令，交给单例 Dispatcher
                default -> dispatcher.dispatch(commandName, array, ctx);
            };

            // 如果返回 null (如 BLPOP 阻塞中)，则不写回
            if (response != null) {
                ctx.writeAndFlush(response);
            }
        } catch (Exception e) {
            log.error("Error processing command {}", commandName, e);
            ctx.writeAndFlush(new ErrorMessage("ERR internal error"));
        }
    }

    // --- Mock 处理逻辑 ---

    private RedisMessage handleInfo(RedisMessage[] elements) {
        long uptime = ManagementFactory.getRuntimeMXBean().getUptime() / 1000;
        long pid = ProcessHandle.current().pid();

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
                """.formatted(
                System.getProperty("os.name"),
                pid,
                uptime,
                uptime / (3600 * 24),
                connectedClients
        );

        return new BulkString(info);
    }

    private RedisMessage handleScanMock(RedisMessage[] elements) {
        // 返回游标 0 和一些假 Key
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
