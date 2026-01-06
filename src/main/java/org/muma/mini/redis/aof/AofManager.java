package org.muma.mini.redis.aof;

import org.muma.mini.redis.config.MiniRedisConfig;
import org.muma.mini.redis.protocol.BulkString;
import org.muma.mini.redis.protocol.RedisArray;
import org.muma.mini.redis.protocol.RedisMessage;
import org.muma.mini.redis.protocol.RespEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

/**
 * AOF 逻辑管理器
 * 负责 Manifest 管理、文件轮转、命令追加。
 */
public class AofManager {

    private static final Logger log = LoggerFactory.getLogger(AofManager.class);

    private static final byte[] CRLF = "\r\n".getBytes(StandardCharsets.UTF_8);

    private final MiniRedisConfig config;
    private final AofDiskWriter diskWriter;
    private final AofManifest manifest;

    // RESP 编码器工具，用于将 RedisArray 转回字节流
    // 注意：我们需要一个不依赖 Netty Channel 的编码工具方法
    // 或者我们自己简单拼装一下 RESP

    public AofManager(MiniRedisConfig config) {
        this.config = config;
        this.diskWriter = new AofDiskWriter(config);
        this.manifest = new AofManifest();
    }

    /**
     * 初始化 AOF 子系统
     * 1. 如果开启了 AOF，加载 Manifest
     * 2. 打开当前的 Incr 文件准备写入
     */
    public void init() {
        if (!config.isAppendOnly()) return;

        try {
            loadManifest();

            // 确保有一个 Incr 文件可写
            AofManifest.AofInfo lastIncr = manifest.getLastIncrAof();
            if (lastIncr == null) {
                // 初始化第一个 Incr 文件
                long newSeq = manifest.nextSeq();
                String filename = config.getAppendFilename() + "." + newSeq + ".incr.aof";
                manifest.addIncrAof(filename, newSeq);

                // 持久化 Manifest
                saveManifest();
                lastIncr = manifest.getLastIncrAof();
            }

            // 打开文件
            diskWriter.open(lastIncr.filename);

        } catch (IOException e) {
            log.error("Failed to initialize AOF", e);
            throw new RuntimeException("AOF init failed", e);
        }
    }

    /**
     * 追加一条写命令
     * 该方法由主线程调用，必须非阻塞且极快。
     */
    public void append(RedisArray command) {
        if (!config.isAppendOnly()) return;

        try {
            // 1. 将命令序列化为 RESP 字节流
            // TODO: 这里需要一个高效的序列化方法
            byte[] rawBytes = encodeCommand(command);

            // 2. 写入 DiskWriter (写入 PageCache)
            diskWriter.write(rawBytes);

            // 3. (可选) 写入时间戳
            // 实际 Redis 不会每条都写，而是每隔一定时间。
            // 这里简化：暂不写，或者每 N 条写一次。

        } catch (IOException e) {
            log.error("Failed to append AOF", e);
            // 生产环境这里可能需要降级或报警
        }
    }

    public void shutdown() {
        if (config.isAppendOnly()) {
            diskWriter.shutdown();
        }
    }

    // --- 内部辅助 ---

    private void loadManifest() throws IOException {
        File dir = new File(config.getAppendDir());
        if (!dir.exists()) dir.mkdirs();

        File manifestFile = new File(dir, "appendonly.aof.manifest");
        if (manifestFile.exists()) {
            String content = Files.readString(manifestFile.toPath());
            AofManifest loaded = AofManifest.decode(content);

            // 简单的属性拷贝，或者直接替换 this.manifest
            // 这里为了简单，我们手动同步一下状态 (生产环境建议直接用 loaded 替换)
            if (loaded.getBaseAof() != null) {
                this.manifest.setBaseAof(loaded.getBaseAof().filename, loaded.getBaseAof().seq);
            }
            for (AofManifest.AofInfo info : loaded.getIncrAofs()) {
                this.manifest.addIncrAof(info.filename, info.seq);
            }
        }
    }

    private void saveManifest() throws IOException {
        File dir = new File(config.getAppendDir());
        File manifestFile = new File(dir, "appendonly.aof.manifest");

        String content = manifest.encode();
        // 原子写：先写 temp 再 rename (这是标准做法)
        // 这里简化直接写
        Files.writeString(manifestFile.toPath(), content);
    }

    /**
     * 简单的 RESP 序列化工具
     * *3\r\n$3\r\nSET\r\n...
     */
    public static byte[] encodeCommand(RedisArray array) {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
            // *<count>\r\n
            bos.write('*');
            bos.write(String.valueOf(array.elements().length).getBytes());
            bos.write(CRLF);

            for (RedisMessage msg : array.elements()) {
                if (msg instanceof BulkString bs) {
                    bos.write('$');
                    if (bs.content() == null) {
                        bos.write("-1".getBytes());
                    } else {
                        bos.write(String.valueOf(bs.content().length).getBytes());
                        bos.write(CRLF);
                        bos.write(bs.content());
                    }
                    bos.write(CRLF);
                } else {
                    // 理论上 Command 参数都是 BulkString，这里防御一下
                    throw new IllegalArgumentException("Unsupported type in AOF: " + msg.getClass());
                }
            }
            return bos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e); // BAOS 不会抛 IOE
        }
    }
}
