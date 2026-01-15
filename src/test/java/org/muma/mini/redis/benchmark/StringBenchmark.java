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
 * String 模块专项基准测试
 * 涵盖 SET, GET, INCR, MGET, SETBIT
 */
public class StringBenchmark {

    private static final String HOST = "127.0.0.1";
    private static final int PORT = 6379;

    private static final int CONCURRENCY = 50;
    private static final int REQUESTS_PER_CLIENT = 10000;
    private static final int TOTAL_REQUESTS = CONCURRENCY * REQUESTS_PER_CLIENT;

    private static final int WARMUP_ROUNDS = 3;
    private static final int MEASURE_ROUNDS = 5;

    // --- Payload Construction ---

    // 1. SET key val
    private static final ByteBuf SET_CMD = buf("*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$3\r\nval\r\n");

    // 2. GET key
    private static final ByteBuf GET_CMD = buf("*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n");

    // 3. INCR counter (需预先 SET counter 0)
    private static final ByteBuf INCR_CMD = buf("*2\r\n$4\r\nINCR\r\n$7\r\ncounter\r\n");

    // 4. MGET k1 k2 k3 (需预先 SET)
    private static final ByteBuf MGET_CMD = buf("*4\r\n$4\r\nMGET\r\n$2\r\nk1\r\n$2\r\nk2\r\n$2\r\nk3\r\n");

    // 5. SETBIT bitmap 100 1
    private static final ByteBuf SETBIT_CMD = buf("*4\r\n$6\r\nSETBIT\r\n$6\r\nbitmap\r\n$3\r\n100\r\n$1\r\n1\r\n");

    private static ByteBuf buf(String cmd) {
        return Unpooled.unreleasableBuffer(Unpooled.wrappedBuffer(cmd.getBytes(StandardCharsets.UTF_8)));
    }

    public static void main(String[] args) throws InterruptedException {
        System.out.println("========== String Benchmark ==========");
        EventLoopGroup group = new NioEventLoopGroup();

        try {
            // Pre-populate data
            System.out.println("Pre-populating data...");
            // 这里简单发一次预热数据，保证 Get 不会全空
            // 实际压测时，SET 压测本身就会填充 key
            // 我们手动初始化 counter 和 mget keys
            // (略：可通过 redis-cli 或简单的 Netty 发送)

            // --- Benchmark Start ---

            runStableBenchmark(group, "SET", SET_CMD);
            runStableBenchmark(group, "GET", GET_CMD);

            // 压测 INCR 前建议先 SET counter 0，或者直接压测（会自动创建）
            runStableBenchmark(group, "INCR", INCR_CMD);

            runStableBenchmark(group, "MGET", MGET_CMD);

            runStableBenchmark(group, "SETBIT", SETBIT_CMD);

        } finally {
            group.shutdownGracefully();
        }
    }

    private static void runStableBenchmark(EventLoopGroup group, String title, ByteBuf command) throws InterruptedException {
        System.out.println("\n>>> Benchmarking " + title + " <<<");

        // Warmup
        System.out.print("Warming up... ");
        for (int i = 0; i < WARMUP_ROUNDS; i++) {
            runRound(group, command);
            System.out.print((i + 1) + " ");
        }
        System.out.println("Done.");

        // Measure
        List<Double> results = new ArrayList<>();
        System.out.println("Measuring rounds:");

        for (int i = 0; i < MEASURE_ROUNDS; i++) {
            Thread.sleep(200);
            double qps = runRound(group, command);
            results.add(qps);
            System.out.printf("Round %d: %.2f QPS%n", i + 1, qps);
        }

        // Stats
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
