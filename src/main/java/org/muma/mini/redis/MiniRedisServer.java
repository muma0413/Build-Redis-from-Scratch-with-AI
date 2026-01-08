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
import org.muma.mini.redis.config.MiniRedisConfig;
import org.muma.mini.redis.protocol.RespDecoder;
import org.muma.mini.redis.protocol.RespEncoder;
import org.muma.mini.redis.server.RedisCommandHandler;
import org.muma.mini.redis.server.RedisServerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MiniRedisServer {

    private static final Logger log = LoggerFactory.getLogger(MiniRedisServer.class);
    private final int port;

    public MiniRedisServer(int port) {
        this.port = port;
    }

    public void start() throws InterruptedException {
        // 1. 初始化业务上下文
        MiniRedisConfig config = MiniRedisConfig.getInstance();
        RedisServerContext serverContext = new RedisServerContext(config);
        serverContext.init(); // 加载数据、启动后台线程

        // 2. 启动网络层
        EventLoopGroup bossGroup = new NioEventLoopGroup(1);
        EventLoopGroup workerGroup = new NioEventLoopGroup(); // 默认 CPU*2

        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .handler(new LoggingHandler(LogLevel.INFO))
                    .childOption(ChannelOption.TCP_NODELAY, true)
                    .childOption(ChannelOption.SO_KEEPALIVE, true)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ch.pipeline()
                                    .addLast(new RespDecoder())
                                    .addLast(new RespEncoder())
                                    // 这里的 handler 依然是每连接 new 一个，但传入单例组件
                                    .addLast(new RedisCommandHandler(
                                            serverContext.getDispatcher(),
                                            serverContext.getCoreExecutor()
                                    ));
                        }
                    });

            log.info("Starting Mini-Redis server on port {}", port);
            ChannelFuture future = b.bind(port).sync();
            log.info("Mini-Redis started successfully.");

            future.channel().closeFuture().sync();
        } catch (Exception e) {
            log.error("Failed to start server", e);
        } finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
            serverContext.shutdown(); // 确保优雅关闭业务组件
        }
    }

    public static void main(String[] args) throws InterruptedException {
        MiniRedisConfig config = MiniRedisConfig.getInstance();
        config.parseArgs(args);
        new MiniRedisServer(config.getPort()).start();
    }
}
