package org.muma.mini.redis.aof;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.muma.mini.redis.protocol.BulkString;
import org.muma.mini.redis.protocol.RedisArray;
import org.muma.mini.redis.protocol.RedisMessage;
import org.muma.mini.redis.utils.RespCodecUtil;

import java.nio.charset.StandardCharsets;

public class AofTestClient {

    private static final String HOST = "127.0.0.1";
    private static final int PORT = 6379;
    private static final int COUNT = 10000;

    public static void main(String[] args) throws InterruptedException {
        String mode = args.length > 0 ? args[0] : "VERIFY"; // WRITE or VERIFY

        if ("WRITE".equalsIgnoreCase(mode)) {
            write();
        } else {
            verify();
        }
    }

    private static void write() throws InterruptedException {
        System.out.println(">>> Writing " + COUNT + " keys...");
        EventLoopGroup group = new NioEventLoopGroup();
        try {
            Bootstrap b = new Bootstrap();
            b.group(group).channel(NioSocketChannel.class).handler(new ChannelInitializer<SocketChannel>() {
                @Override
                protected void initChannel(SocketChannel ch) {
                    ch.pipeline().addLast(new SimpleChannelInboundHandler<ByteBuf>() {
                        @Override
                        public void channelActive(ChannelHandlerContext ctx) {
                            // 构造 RedisArray 对象
                            for (int i = 0; i < COUNT; i++) {
                                RedisArray cmd = new RedisArray(new RedisMessage[]{
                                        new BulkString("SET"),
                                        new BulkString("key:" + i),
                                        new BulkString("val:" + i)
                                });

                                // 使用工具类转字节
                                byte[] bytes = RespCodecUtil.encode(cmd);
                                ctx.write(Unpooled.wrappedBuffer(bytes));
                            }
                            ctx.flush();
                        }

                        @Override
                        protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) {
                        } // Ignore response
                    });
                }
            });
            Channel ch = b.connect(HOST, PORT).sync().channel();
            // 等待发送完成 (简单 sleep)
            Thread.sleep(2000);
            ch.close();
        } finally {
            group.shutdownGracefully();
        }
        System.out.println("Done.");
    }

    private static void verify() throws InterruptedException {
        System.out.println(">>> Verifying data...");
        EventLoopGroup group = new NioEventLoopGroup();
        try {
            Bootstrap b = new Bootstrap();
            b.group(group).channel(NioSocketChannel.class).handler(new ChannelInitializer<SocketChannel>() {
                @Override
                protected void initChannel(SocketChannel ch) {
                    ch.pipeline().addLast(new SimpleChannelInboundHandler<ByteBuf>() {
                        int received = 0;

                        @Override
                        public void channelActive(ChannelHandlerContext ctx) {
                            // 发送 DBSIZE
                            String cmd = "*1\r\n$6\r\nDBSIZE\r\n";
                            ctx.writeAndFlush(Unpooled.wrappedBuffer(cmd.getBytes(StandardCharsets.UTF_8)));
                        }

                        @Override
                        protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) {
                            // 解析 :10000\r\n
                            String resp = msg.toString(StandardCharsets.UTF_8);
                            System.out.println("Server Response: " + resp);
                            if (resp.contains(":" + COUNT)) {
                                System.out.println("✅ SUCCESS: Data count matches!");
                            } else {
                                System.err.println("❌ FAIL: Expected " + COUNT);
                            }
                            ctx.close();
                        }
                    });
                }
            });
            b.connect(HOST, PORT).sync().channel().closeFuture().sync();
        } finally {
            group.shutdownGracefully();
        }
    }
}
