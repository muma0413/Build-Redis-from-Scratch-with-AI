package org.muma.mini.redis.benchmark;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * ZSet 模块专项基准测试 (Top 5 Commands)
 * ZADD, ZSCORE, ZRANGE, ZCOUNT, ZREVRANGE
 */
public class ZSetBenchmark {

    private static final String HOST = "127.0.0.1";
    private static final int PORT = 6379;

    private static final int CONCURRENCY = 50;
    private static final int REQUESTS_PER_CLIENT = 10000;
    private static final int TOTAL_REQUESTS = CONCURRENCY * REQUESTS_PER_CLIENT;

    private static final int WARMUP_ROUNDS = 3;
    private static final int MEASURE_ROUNDS = 5;

    // --- Payload Construction ---

    // 1. ZADD myzset 100 val (写)
    // 为了避免一直写同一个值导致 ZSet 不增长，最好是在循环里动态生成
    // 但为了 Benchmark 简单，我们先写死，测试更新性能 (Update)
    // 如果要测插入性能 (Insert)，需要预热大量数据
    private static final ByteBuf ZADD_CMD = buf("*4\r\n$4\r\nZADD\r\n$6\r\nmyzset\r\n$3\r\n100\r\n$3\r\nval\r\n");

    // 2. ZSCORE myzset val (O(1) 查)
    private static final ByteBuf ZSCORE_CMD = buf("*3\r\n$6\r\nZSCORE\r\n$6\r\nmyzset\r\n$3\r\nval\r\n");

    // 3. ZRANGE myzset 0 10 (O(logN + M) 查)
    private static final ByteBuf ZRANGE_CMD = buf("*4\r\n$6\r\nZRANGE\r\n$6\r\nmyzset\r\n$1\r\n0\r\n$2\r\n10\r\n");

    // 4. ZCOUNT myzset 0 100 (O(logN) 查)
    private static final ByteBuf ZCOUNT_CMD = buf("*4\r\n$6\r\nZCOUNT\r\n$6\r\nmyzset\r\n$1\r\n0\r\n$3\r\n100\r\n");

    // 5. ZREVRANGE myzset 0 10 (倒序查)
    private static final ByteBuf ZREVRANGE_CMD = buf("*4\r\n$9\r\nZREVRANGE\r\n$6\r\nmyzset\r\n$1\r\n0\r\n$2\r\n10\r\n");

    private static ByteBuf buf(String cmd) {
        return Unpooled.unreleasableBuffer(Unpooled.wrappedBuffer(cmd.getBytes(StandardCharsets.UTF_8)));
    }

    public static void main(String[] args) throws InterruptedException {
        System.out.println("========== ZSet Benchmark ==========");
        System.out.println("Config: " + CONCURRENCY + " clients, " + REQUESTS_PER_CLIENT + " reqs/client");
        System.out.println("Strategy: " + WARMUP_ROUNDS + " Warmup + " + MEASURE_ROUNDS + " Measure Rounds");
        System.out.println("------------------------------------");

        EventLoopGroup group = new NioEventLoopGroup(Runtime.getRuntime().availableProcessors());

        try {
            // Pre-populate if needed
            // (如果不预热数据，ZRANGE 测的是空集，QPS 会虚高)
            // 建议：在运行前，用 redis-cli 塞个几万条数据进去，或者依赖 ZADD 压测残留

            runStableBenchmark(group, "ZADD", ZADD_CMD);
            runStableBenchmark(group, "ZSCORE", ZSCORE_CMD);
            runStableBenchmark(group, "ZRANGE", ZRANGE_CMD);
            runStableBenchmark(group, "ZCOUNT", ZCOUNT_CMD);
            runStableBenchmark(group, "ZREVRANGE", ZREVRANGE_CMD);

        } finally {
            group.shutdownGracefully();
        }
    }

    // ... runStableBenchmark & BenchmarkHandler (保持不变) ...
    // 请直接复制 HashBenchmark 中的实现
    private static void runStableBenchmark(EventLoopGroup group, String title, ByteBuf command) throws InterruptedException {
        System.out.println("\n>>> Benchmarking " + title + " <<<");
        System.out.print("Warming up... ");
        for (int i = 0; i < WARMUP_ROUNDS; i++) {
            runRound(group, command);
            System.out.print((i + 1) + " ");
        }
        System.out.println("Done.");

        List<Double> results = new ArrayList<>();
        System.out.println("Measuring rounds:");

        for (int i = 0; i < MEASURE_ROUNDS; i++) {
            Thread.sleep(200);
            double qps = runRound(group, command);
            results.add(qps);
            System.out.printf("Round %d: %.2f QPS%n", i + 1, qps);
        }

        if (results.size() >= 3) {
            Collections.sort(results);
            results.remove(0);
            results.remove(results.size() - 1);
        }

        double avgQps = results.stream().mapToDouble(Double::doubleValue).average().orElse(0);
        System.out.printf(">>> Final Result [%s]: Avg QPS = %.2f%n", title, avgQps);
    }

    private static double runRound(EventLoopGroup group, ByteBuf command) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(CONCURRENCY);
        long startTime = System.nanoTime();

        Bootstrap b = new Bootstrap();
        b.group(group)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
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
        return TOTAL_REQUESTS / seconds;
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
            while (msg.readableBytes() > 0) {
                if (msg.readByte() == '\n') {
                    received++;
                    if (received >= REQUESTS_PER_CLIENT) {
                        ctx.close();
                        latch.countDown();
                        return;
                    }
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
