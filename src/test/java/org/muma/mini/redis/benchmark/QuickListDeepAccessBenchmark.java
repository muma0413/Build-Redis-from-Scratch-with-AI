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

public class QuickListDeepAccessBenchmark {

    private static final String HOST = "127.0.0.1";
    private static final int PORT = 6379;
    private static final int CONCURRENCY = 50;
    private static final int REQUESTS_PER_CLIENT = 5000; // 单轮请求数
    private static final int TOTAL_REQUESTS = CONCURRENCY * REQUESTS_PER_CLIENT;

    // 配置：预热轮数和正式轮数
    private static final int WARMUP_ROUNDS = 3;
    private static final int MEASURE_ROUNDS = 5;

    private static final ByteBuf LINDEX_TAIL_CMD = buf("*3\r\n$6\r\nLINDEX\r\n$6\r\nmylist\r\n$5\r\n19999\r\n");
    private static final ByteBuf LSET_TAIL_CMD = buf("*4\r\n$4\r\nLSET\r\n$6\r\nmylist\r\n$5\r\n19999\r\n$3\r\nnew\r\n");

    private static ByteBuf buf(String cmd) {
        return Unpooled.unreleasableBuffer(Unpooled.wrappedBuffer(cmd.getBytes(StandardCharsets.UTF_8)));
    }

    public static void main(String[] args) throws InterruptedException {
        EventLoopGroup group = new NioEventLoopGroup();

        try {
            System.out.println("========== Stability Benchmark ==========");
            System.out.println("Config: " + WARMUP_ROUNDS + " Warmup + " + MEASURE_ROUNDS + " Measure Rounds");

            // 1. 测试 LINDEX
            System.out.println("\n--- Benchmarking LINDEX Tail ---");
            runStableBenchmark(group, "LINDEX", LINDEX_TAIL_CMD);

            // 2. 测试 LSET
            System.out.println("\n--- Benchmarking LSET Tail ---");
            runStableBenchmark(group, "LSET", LSET_TAIL_CMD);

        } finally {
            group.shutdownGracefully();
        }
    }

    private static void runStableBenchmark(EventLoopGroup group, String title, ByteBuf command) throws InterruptedException {
        // 1. Warmup
        System.out.print("Warming up... ");
        for (int i = 0; i < WARMUP_ROUNDS; i++) {
            runRound(group, command);
            System.out.print((i + 1) + " ");
        }
        System.out.println("Done.");

        // 2. Measure
        List<Double> results = new ArrayList<>();
        System.out.println("Measuring rounds:");

        for (int i = 0; i < MEASURE_ROUNDS; i++) {
            Thread.sleep(500); // 间隔休息
            double qps = runRound(group, command);
            results.add(qps);
            System.out.printf("Round %d: %.2f QPS%n", i + 1, qps);
        }

        // 3. Calculate Stats
        // 去掉最高最低 (如果轮数够多)
        if (results.size() >= 5) {
            Collections.sort(results);
            results.remove(0); // 去掉最低
            results.remove(results.size() - 1); // 去掉最高
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
