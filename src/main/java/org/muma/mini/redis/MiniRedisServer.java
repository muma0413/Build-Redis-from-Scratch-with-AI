package org.muma.mini.redis;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import org.muma.mini.redis.aof.AofLoader;
import org.muma.mini.redis.aof.AofManager;
import org.muma.mini.redis.command.CommandDispatcher;
import org.muma.mini.redis.config.MiniRedisConfig;
import org.muma.mini.redis.server.RedisCommandHandler;
import org.muma.mini.redis.protocol.RespDecoder;
import org.muma.mini.redis.protocol.RespEncoder;
import org.muma.mini.redis.server.RedisCoreExecutor;
import org.muma.mini.redis.store.StorageEngine;
import org.muma.mini.redis.store.impl.MemoryStorageEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MiniRedisServer {

    private static final Logger log = LoggerFactory.getLogger(MiniRedisServer.class);
    private final int port;

    public MiniRedisServer(int port) {
        this.port = port;
    }

    public void start() throws InterruptedException {
        EventLoopGroup bossGroup = new NioEventLoopGroup(1);
        EventLoopGroup workerGroup = new NioEventLoopGroup();

        // 1. 加载配置
        MiniRedisConfig config = MiniRedisConfig.getInstance();

        // 2. 初始化 AOF Manager 并尝试恢复数据
        AofManager aofManager = new AofManager(config);

        // 3. 初始化存储
        MemoryStorageEngine storage = new MemoryStorageEngine();

        storage.setAofManager(aofManager);

        // 1. 在外面初始化单例
        CommandDispatcher dispatcher = new CommandDispatcher(storage, aofManager);

        // 5. 【关键】AOF 恢复数据 (Replay)
        // 必须在 Netty 启动前完成，且此时 dispatcher 已经准备好
        AofLoader loader = new AofLoader(config, dispatcher, storage);
        loader.load(); // 如果有 AOF 文件，这里会把数据灌入 storage

        // 6. 初始化 AOF 写入准备 (打开文件)
        // 必须在 load 之后，否则会覆盖或者冲突？
        // 其实 init 主要负责打开 Incr 文件。应该在 load 之后，准备接收新写请求。
        aofManager.init();

        RedisCoreExecutor coreExecutor = new RedisCoreExecutor(); // 【新增】

        try {
            var bootstrap = new ServerBootstrap();
            bootstrap.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    // 在 Boss 线程增加 Netty 自带的日志 Handler，可以看到 TCP 连接握手细节
                    .handler(new LoggingHandler(LogLevel.INFO))
                    // 开启 TCP_NODELAY (禁用 Nagle 算法)，降低延迟
                    .childOption(ChannelOption.TCP_NODELAY, true)
                    // 开启 SO_KEEPALIVE
                    .childOption(ChannelOption.SO_KEEPALIVE, true)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ch.pipeline()
                                    .addLast(new RespDecoder())
                                    .addLast(new RespEncoder())
                                    .addLast(new RedisCommandHandler(dispatcher, coreExecutor));
                        }
                    });

            log.info("Starting Mini-Redis server on port {}", port);
            ChannelFuture future = bootstrap.bind(port).sync();

            // 记得 shutdown hook 关闭 aofManager
            Runtime.getRuntime().addShutdownHook(new Thread(aofManager::shutdown));

            log.info("Mini-Redis started successfully.");
            future.channel().closeFuture().sync();
        } catch (Exception e) {
            log.error("Failed to start server", e);
        } finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }

    public static void main(String[] args) throws InterruptedException {
        // 1. 初始化配置并解析参数
        MiniRedisConfig config = MiniRedisConfig.getInstance();
        config.parseArgs(args);
        new MiniRedisServer(config.getPort()).start();
    }
}
