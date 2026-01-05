package org.muma.mini.redis.benchmark;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

public class MiniRedisBenchmark {

    private static final String HOST = "127.0.0.1";
    private static final int PORT = 6379;

    private static final int CONCURRENCY = 50;
    private static final int REQUESTS_PER_CLIENT = 10000;
    private static final int TOTAL_REQUESTS = CONCURRENCY * REQUESTS_PER_CLIENT;

    // --- Commands Pre-allocation ---

    // 1. String
    private static final ByteBuf SET_CMD = buf("*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n");
    private static final ByteBuf GET_CMD = buf("*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n");

    // 2. Hash
    private static final ByteBuf HSET_CMD = buf("*4\r\n$4\r\nHSET\r\n$6\r\nmyhash\r\n$6\r\nfield1\r\n$4\r\nval1\r\n");
    private static final ByteBuf HGET_CMD = buf("*3\r\n$4\r\nHGET\r\n$6\r\nmyhash\r\n$6\r\nfield1\r\n");

    // 3. List
    private static final ByteBuf LPUSH_CMD = buf("*3\r\n$5\r\nLPUSH\r\n$6\r\nmylist\r\n$3\r\nval\r\n");
    private static final ByteBuf LPOP_CMD = buf("*2\r\n$4\r\nLPOP\r\n$6\r\nmylist\r\n");
    // 注意：LPOP 测多了 List 会空，建议配合 LPUSH 或者用 LRANGE 测读
    private static final ByteBuf LRANGE_CMD = buf("*4\r\n$6\r\nLRANGE\r\n$6\r\nmylist\r\n$1\r\n0\r\n$2\r\n10\r\n");

    // 4. Set
    private static final ByteBuf SADD_CMD = buf("*3\r\n$4\r\nSADD\r\n$5\r\nmyset\r\n$3\r\nval\r\n");
    private static final ByteBuf SISMEMBER_CMD = buf("*3\r\n$9\r\nSISMEMBER\r\n$5\r\nmyset\r\n$3\r\nval\r\n");

    // 5. ZSet
    private static final ByteBuf ZADD_CMD = buf("*4\r\n$4\r\nZADD\r\n$6\r\nmyzset\r\n$3\r\n100\r\n$3\r\nval\r\n");
    private static final ByteBuf ZRANGE_CMD = buf("*4\r\n$6\r\nZRANGE\r\n$6\r\nmyzset\r\n$1\r\n0\r\n$2\r\n10\r\n");

    private static ByteBuf buf(String cmd) {
        return Unpooled.unreleasableBuffer(Unpooled.wrappedBuffer(cmd.getBytes(StandardCharsets.UTF_8)));
    }

    public static void main(String[] args) throws InterruptedException {
        System.out.println("========== Mini-Redis Benchmark ==========");
        System.out.println("Concurrency: " + CONCURRENCY);
        System.out.println("Total Requests: " + TOTAL_REQUESTS);

        EventLoopGroup group = new NioEventLoopGroup(Runtime.getRuntime().availableProcessors());

        try {
            // String
            runBenchmark(group, "SET", SET_CMD);
            runBenchmark(group, "GET", GET_CMD);

            // Hash
            runBenchmark(group, "HSET", HSET_CMD);
            runBenchmark(group, "HGET", HGET_CMD);

            // List
            runBenchmark(group, "LPUSH", LPUSH_CMD);
            runBenchmark(group, "LRANGE", LRANGE_CMD); // 读操作测 LRANGE 更合适

            // Set
            runBenchmark(group, "SADD", SADD_CMD);
            runBenchmark(group, "SISMEMBER", SISMEMBER_CMD);

            // ZSet
            runBenchmark(group, "ZADD", ZADD_CMD);
            runBenchmark(group, "ZRANGE", ZRANGE_CMD);

        } finally {
            group.shutdownGracefully();
        }
    }

    private static void runBenchmark(EventLoopGroup group, String title, ByteBuf command) throws InterruptedException {
        // 先休眠一下，让系统 calm down，避免上次 GC 影响
        Thread.sleep(500);

        CountDownLatch latch = new CountDownLatch(CONCURRENCY);
        long startTime = System.nanoTime();

        Bootstrap b = new Bootstrap();
        b.group(group)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true) // 禁用 Nagle 算法，降低延迟
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.pipeline().addLast(new BenchmarkHandler(command, latch));
                    }
                });

        for (int i = 0; i < CONCURRENCY; i++) {
            b.connect(HOST, PORT);
        }

        latch.await();

        long durationNs = System.nanoTime() - startTime;
        double seconds = durationNs / 1_000_000_000.0;
        double qps = TOTAL_REQUESTS / seconds;

        System.out.printf("Test: %-10s | Duration: %.2fs | QPS: %.2f%n", title, seconds, qps);
    }

    static class BenchmarkHandler extends SimpleChannelInboundHandler<ByteBuf> {
        private final ByteBuf command;
        private final CountDownLatch latch;

        private int sent = 0;
        private int received = 0;

        public BenchmarkHandler(ByteBuf command, CountDownLatch latch) {
            this.command = command;
            this.latch = latch;
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) {
            flushBatch(ctx);
        }

        private void flushBatch(ChannelHandlerContext ctx) {
            // 每次发 50 个，保持 Pipeline 深度，避免 TCP Buffer 溢出
            int batch = Math.min(50, REQUESTS_PER_CLIENT - sent);
            if (batch <= 0) return;

            for (int i = 0; i < batch; i++) {
                ctx.write(command.retainedDuplicate());
                sent++;
            }
            ctx.flush();
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) {
            // 简单解析换行符来计数
            while (msg.readableBytes() > 0) {
                if (msg.readByte() == '\n') {
                    received++;
                    if (received >= REQUESTS_PER_CLIENT) {
                        ctx.close();
                        latch.countDown();
                        return;
                    }

                    // 保持 Pipeline 持续流动
                    if (received % 20 == 0 && sent < REQUESTS_PER_CLIENT) {
                        flushBatch(ctx);
                    }
                }
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            ctx.close();
            latch.countDown();
        }
    }
}
