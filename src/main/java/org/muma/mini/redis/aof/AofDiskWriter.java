package org.muma.mini.redis.aof;

import org.muma.mini.redis.config.MiniRedisConfig;
import org.muma.mini.redis.utils.ThreadUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * AOF 物理写入器 (Async IO)
 * <p>
 * 架构升级：
 * 1. 主线程 -> 写入内存队列 (bufferQueue) -> 立即返回 (不阻塞)
 * 2. 后台线程 (AOF-Writer) -> 从队列取数据 -> FileChannel.write -> OS Cache
 * 3. Fsync线程 (AOF-Fsync) -> 定时调用 force() -> Disk
 */
public class AofDiskWriter {

    private static final Logger log = LoggerFactory.getLogger(AofDiskWriter.class);

    private final MiniRedisConfig config;
    private FileChannel fileChannel;

    // 使用有界队列，例如容量 64MB (按平均 64字节一条命令算，约 100万条)
    // 这里简单用条数限制
    private final BlockingQueue<byte[]> bufferQueue = new ArrayBlockingQueue<>(100_000);

    // 后台写入线程
    private final ExecutorService writerExecutor;

    // 定时刷盘线程
    private final ScheduledExecutorService fsyncExecutor;

    // 状态控制
    private final AtomicBoolean isFsyncing = new AtomicBoolean(false);
    private volatile boolean running = true;

    public AofDiskWriter(MiniRedisConfig config) {
        this.config = config;

        // 1. 启动写入线程
        this.writerExecutor = Executors.newSingleThreadExecutor(
                ThreadUtils.namedThreadFactory("AOF-Writer")
        );
        this.writerExecutor.submit(this::writeLoop);

        // 2. 启动刷盘线程 (仅 EVERYSEC 策略)
        if (config.getAppendFsync() == MiniRedisConfig.AppendFsync.EVERYSEC) {
            this.fsyncExecutor = Executors.newSingleThreadScheduledExecutor(
                    ThreadUtils.namedThreadFactory("AOF-Fsync")
            );
            this.fsyncExecutor.scheduleAtFixedRate(this::performFsync, 1, 1, TimeUnit.SECONDS);
        } else {
            this.fsyncExecutor = null;
        }
    }

    /**
     * 打开文件 (由 AofManager 调用)
     */
    public void open(String filename) throws IOException {
        // ... (同前，略微调整同步逻辑)
        // 注意：fileChannel 的切换需要在 writer 线程安全进行，或者加锁
        // 简单起见，我们在主线程 open，但 writeLoop 需要感知
        synchronized (this) {
            closeChannel(); // 关闭旧的

            File dir = new File(config.getAppendDir());
            if (!dir.exists() && !dir.mkdirs()) {
                throw new IOException("Failed to create AOF dir");
            }
            File file = new File(dir, filename);
            this.fileChannel = FileChannel.open(file.toPath(),
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND);

            log.info("Opened AOF file: {}", file.getAbsolutePath());
        }
    }

    /**
     * 提交写入请求 (主线程调用，极快)
     */
    public void write(byte[] content) {
        if (!running) return;
        try {
            // 队列满时阻塞主线程，起到反压作用
            // 这虽然会降低 QPS，但能防止 OOM，是生产环境必须的保护
            bufferQueue.put(content);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public void writeTimestamp() {
        String ts = "#TS:" + (System.currentTimeMillis() / 1000) + "\r\n";
        write(ts.getBytes());
    }

    /**
     * 后台写入循环
     */
    private void writeLoop() {
        while (running) {
            try {
                // 阻塞获取，直到有数据
                byte[] data = bufferQueue.take();

                synchronized (this) { // 保护 fileChannel 引用
                    if (fileChannel != null && fileChannel.isOpen()) {
                        ByteBuffer buf = ByteBuffer.wrap(data);
                        while (buf.hasRemaining()) {
                            fileChannel.write(buf);
                        }

                        // ALWAYS 策略：同步刷盘
                        if (config.getAppendFsync() == MiniRedisConfig.AppendFsync.ALWAYS) {
                            fileChannel.force(false);
                        }
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                log.error("AOF background write error", e);
            }
        }
    }

    private void performFsync() {
        synchronized (this) {
            if (fileChannel == null || !fileChannel.isOpen()) return;
            if (!isFsyncing.compareAndSet(false, true)) return;

            try {
                fileChannel.force(false);
            } catch (IOException e) {
                log.warn("AOF fsync failed", e);
            } finally {
                isFsyncing.set(false);
            }
        }
    }

    public void close() {
        // 等待队列清空？还是强制关闭？
        // 优雅关闭：设置 running=false，等待 loop 结束
        // 这里简化直接关 channel
        synchronized (this) {
            closeChannel();
        }
    }

    private void closeChannel() {
        if (fileChannel != null) {
            try {
                fileChannel.force(true);
                fileChannel.close();
            } catch (IOException e) {
                log.error("Error closing AOF channel", e);
            } finally {
                fileChannel = null;
            }
        }
    }

    public void shutdown() {
        running = false;
        writerExecutor.shutdownNow();
        if (fsyncExecutor != null) fsyncExecutor.shutdownNow();
        close();
    }
}
