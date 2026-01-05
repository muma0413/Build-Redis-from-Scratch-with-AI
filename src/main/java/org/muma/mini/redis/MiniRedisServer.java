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
import org.muma.mini.redis.command.CommandDispatcher;
import org.muma.mini.redis.config.MiniRedisConfig;
import org.muma.mini.redis.server.RedisCommandHandler;
import org.muma.mini.redis.protocol.RespDecoder;
import org.muma.mini.redis.protocol.RespEncoder;
import org.muma.mini.redis.server.RedisCoreExecutor;
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

        // 1. 在外面初始化单例
        CommandDispatcher dispatcher = new CommandDispatcher(new MemoryStorageEngine());
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
