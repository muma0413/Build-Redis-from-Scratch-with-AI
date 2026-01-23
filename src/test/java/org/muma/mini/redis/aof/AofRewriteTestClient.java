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

public class AofRewriteTestClient {

    private static final String HOST = "127.0.0.1";
    private static final int PORT = 6379;
    private static final int COUNT = 2000; // 足够触发 Rewrite

    public static void main(String[] args) throws InterruptedException {
        String mode = args.length > 0 ? args[0] : "VERIFY";

        if ("WRITE".equalsIgnoreCase(mode)) {
            writeAndTriggerRewrite();
        } else {
            verify();
        }
    }

    private static void writeAndTriggerRewrite() throws InterruptedException {
        System.out.println(">>> Writing " + COUNT + " keys to trigger rewrite...");
        EventLoopGroup group = new NioEventLoopGroup();
        try {
            Bootstrap b = new Bootstrap();
            b.group(group).channel(NioSocketChannel.class).handler(new ChannelInitializer<SocketChannel>() {
                @Override
                protected void initChannel(SocketChannel ch) {
                    ch.pipeline().addLast(new SimpleChannelInboundHandler<ByteBuf>() {
                        @Override
                        public void channelActive(ChannelHandlerContext ctx) {
                            for (int i = 0; i < COUNT; i++) {
                                // 构造大量数据，确保体积够大
                                RedisArray cmd = new RedisArray(new RedisMessage[]{
                                        new BulkString("SET"),
                                        new BulkString("key:" + i),
                                        new BulkString("val:" + i + "-payload-padding-padding-padding")
                                });
                                ctx.write(Unpooled.wrappedBuffer(RespCodecUtil.encode(cmd)));
                            }
                            ctx.flush();
                        }
                        @Override
                        protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) {}
                    });
                }
            });
            Channel ch = b.connect(HOST, PORT).sync().channel();

            // 等待发送完成
            Thread.sleep(2000);
            ch.close();

            System.out.println(">>> Writing done. Waiting 5s for background rewrite...");
            Thread.sleep(5000); // 给后台线程一点时间去 Rewrite
            System.out.println(">>> Done. Check your logs and disk!");

        } finally {
            group.shutdownGracefully();
        }
    }

    private static void verify() throws InterruptedException {
        // ... (复用之前的 verify 逻辑，检查 DBSIZE 是否为 COUNT) ...
        System.out.println(">>> Verifying data...");
        EventLoopGroup group = new NioEventLoopGroup();
        try {
            Bootstrap b = new Bootstrap();
            b.group(group).channel(NioSocketChannel.class).handler(new ChannelInitializer<SocketChannel>() {
                @Override
                protected void initChannel(SocketChannel ch) {
                    ch.pipeline().addLast(new SimpleChannelInboundHandler<ByteBuf>() {
                        @Override
                        public void channelActive(ChannelHandlerContext ctx) {
                            RedisArray cmd = new RedisArray(new RedisMessage[]{new BulkString("DBSIZE")});
                            ctx.writeAndFlush(Unpooled.wrappedBuffer(RespCodecUtil.encode(cmd)));
                        }
                        @Override
                        protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) {
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
