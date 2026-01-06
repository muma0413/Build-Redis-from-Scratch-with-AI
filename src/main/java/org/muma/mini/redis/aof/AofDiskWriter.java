package org.muma.mini.redis.aof;

import org.muma.mini.redis.config.MiniRedisConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * AOF 物理写入器 (Single Thread Writer)
 * 负责管理当前正在写入的 Incr 文件，并执行 fsync 策略。
 */
public class AofDiskWriter {

    private static final Logger log = LoggerFactory.getLogger(AofDiskWriter.class);

    private final MiniRedisConfig config;
    private FileChannel fileChannel;
    private final ScheduledExecutorService fsyncExecutor;

    // 用于 everysec 策略的防重入标记
    private final AtomicBoolean isFsyncing = new AtomicBoolean(false);

    public AofDiskWriter(MiniRedisConfig config) {
        this.config = config;

        // 只有 everysec 需要定时任务
        if (config.getAppendFsync() == MiniRedisConfig.AppendFsync.EVERYSEC) {
            this.fsyncExecutor = Executors.newSingleThreadScheduledExecutor(r -> new Thread(r, "AOF-Fsync-Thread"));
            this.fsyncExecutor.scheduleAtFixedRate(this::performFsync, 1, 1, TimeUnit.SECONDS);
        } else {
            this.fsyncExecutor = null;
        }
    }

    /**
     * 打开一个新的 AOF 文件 (通常是 Incr 文件)
     * 会自动创建目录。如果之前有打开的文件，会先关闭。
     */
    public void open(String filename) throws IOException {
        close(); // 确保上一个文件已关闭

        File dir = new File(config.getAppendDir());
        if (!dir.exists()) {
            if (!dir.mkdirs()) {
                throw new IOException("Failed to create AOF directory: " + dir.getAbsolutePath());
            }
        }

        File file = new File(dir, filename);

        // 打开文件通道 (CREATE, APPEND)
        this.fileChannel = FileChannel.open(file.toPath(),
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.APPEND);

        log.info("Opened AOF file: {}", file.getAbsolutePath());
    }

    /**
     * 写入 RESP 数据
     *
     * @param content 原始的 RESP 字节数组 (包含 \r\n)
     */
    public void write(byte[] content) throws IOException {
        if (fileChannel == null) return; // 还没 open，或者 AOF 关闭中

        // 1. 写入 OS Page Cache
        ByteBuffer buf = ByteBuffer.wrap(content);
        while (buf.hasRemaining()) {
            fileChannel.write(buf);
        }

        // 2. 根据策略执行 fsync
        if (config.getAppendFsync() == MiniRedisConfig.AppendFsync.ALWAYS) {
            fileChannel.force(false); // 强制刷盘
        }
    }

    /**
     * 写入时间戳注解 (PITR 支持)
     * 格式: #TS:1678888888\r\n
     */
    public void writeTimestamp() throws IOException {
        if (fileChannel == null) return;

        String ts = "#TS:" + (System.currentTimeMillis() / 1000) + "\r\n";
        write(ts.getBytes());
    }

    public void close() throws IOException {
        if (fileChannel != null) {
            try {
                fileChannel.force(true); // 关闭前强制刷盘
                fileChannel.close();
            } finally {
                fileChannel = null;
            }
        }
    }

    public void shutdown() {
        if (fsyncExecutor != null) {
            fsyncExecutor.shutdown();
        }
        try {
            close();
        } catch (IOException e) {
            log.error("Error closing AOF writer", e);
        }
    }

    // --- Fsync Task ---

    private void performFsync() {
        if (fileChannel == null || !fileChannel.isOpen()) return;

        // 防止重入 (例如上次 fsync 卡住了)
        if (!isFsyncing.compareAndSet(false, true)) return;

        try {
            // force(false) 只刷新内容，不刷新元数据，性能稍好且足够安全
            fileChannel.force(false);
        } catch (IOException e) {
            log.warn("AOF fsync failed", e);
        } finally {
            isFsyncing.set(false);
        }
    }
}