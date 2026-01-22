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
 * Hash 模块专项基准测试 (Top 5 Commands)
 * HSET, HGET, HMGET, HINCRBY, HSCAN
 */
public class HashBenchmark {

    private static final String HOST = "127.0.0.1";
    private static final int PORT = 6379;

    private static final int CONCURRENCY = 50;
    private static final int REQUESTS_PER_CLIENT = 10000;
    private static final int TOTAL_REQUESTS = CONCURRENCY * REQUESTS_PER_CLIENT;

    private static final int WARMUP_ROUNDS = 3;
    private static final int MEASURE_ROUNDS = 5;

    // --- Payload Construction ---

    // 1. HSET myhash field val
    private static final ByteBuf HSET_CMD = buf("*4\r\n$4\r\nHSET\r\n$6\r\nmyhash\r\n$6\r\nfield1\r\n$4\r\nval1\r\n");

    // 2. HGET myhash field
    private static final ByteBuf HGET_CMD = buf("*3\r\n$4\r\nHGET\r\n$6\r\nmyhash\r\n$6\r\nfield1\r\n");

    // 3. HMGET myhash f1 f2 f3 (模拟批量读)
    private static final ByteBuf HMGET_CMD = buf("*5\r\n$5\r\nHMGET\r\n$6\r\nmyhash\r\n$6\r\nfield1\r\n$6\r\nfield2\r\n$6\r\nfield3\r\n");

    // 4. HINCRBY counter f 1 (模拟计数器)
    private static final ByteBuf HINCRBY_CMD = buf("*4\r\n$7\r\nHINCRBY\r\n$7\r\ncounter\r\n$1\r\nf\r\n$1\r\n1\r\n");

    // 5. HSCAN myhash 0 (模拟遍历)
    private static final ByteBuf HSCAN_CMD = buf("*3\r\n$5\r\nHSCAN\r\n$6\r\nmyhash\r\n$1\r\n0\r\n");

    private static ByteBuf buf(String cmd) {
        return Unpooled.unreleasableBuffer(Unpooled.wrappedBuffer(cmd.getBytes(StandardCharsets.UTF_8)));
    }

    public static void main(String[] args) throws InterruptedException {
        System.out.println("========== Hash Benchmark ==========");
        System.out.println("Config: " + CONCURRENCY + " clients, " + REQUESTS_PER_CLIENT + " reqs/client");
        System.out.println("Strategy: " + WARMUP_ROUNDS + " Warmup + " + MEASURE_ROUNDS + " Measure Rounds");
        System.out.println("------------------------------------");

        EventLoopGroup group = new NioEventLoopGroup(Runtime.getRuntime().availableProcessors());

        try {
            // Pre-populate data for HMGET
            System.out.println("Pre-populating data...");
            // (Optional: 可以在这里发几个 HSET field2 field3，但 HSET 压测本身就会填数据，只是只有 field1)
            // 为了 HMGET 有数据，我们可以利用 HSET 压测后的残留数据

            runStableBenchmark(group, "HSET", HSET_CMD);
            runStableBenchmark(group, "HGET", HGET_CMD);

            // 注意：HMGET 可能会读到 nil (如果 field2/3 没 set 过)，但不影响 QPS 测试
            runStableBenchmark(group, "HMGET", HMGET_CMD);

            runStableBenchmark(group, "HINCRBY", HINCRBY_CMD);

            runStableBenchmark(group, "HSCAN", HSCAN_CMD);

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
