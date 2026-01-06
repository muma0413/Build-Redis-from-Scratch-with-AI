package org.muma.mini.redis.aof;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.muma.mini.redis.command.CommandDispatcher;
import org.muma.mini.redis.config.MiniRedisConfig;
import org.muma.mini.redis.protocol.RedisArray;
import org.muma.mini.redis.protocol.RedisMessage;
import org.muma.mini.redis.protocol.RespDecoder;
import org.muma.mini.redis.server.RedisContext;
import org.muma.mini.redis.store.StorageEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;

/**
 * AOF 加载器 (Recovery)
 * 负责在启动时重放 AOF 文件。
 */
public class AofLoader {

    private static final Logger log = LoggerFactory.getLogger(AofLoader.class);

    private final MiniRedisConfig config;
    private final CommandDispatcher dispatcher;
    private final StorageEngine storage;

    public AofLoader(MiniRedisConfig config, CommandDispatcher dispatcher, StorageEngine storage) {
        this.config = config;
        this.dispatcher = dispatcher;
        this.storage = storage;
    }

    public void load() {
        if (!config.isAppendOnly()) return;

        File dir = new File(config.getAppendDir());
        File manifestFile = new File(dir, "appendonly.aof.manifest");
        if (!manifestFile.exists()) {
            log.info("No AOF manifest found, skipping load.");
            return;
        }

        try {
            String content = Files.readString(manifestFile.toPath());
            AofManifest manifest = AofManifest.decode(content);

            // 1. 加载 Base AOF (如果有)
            if (manifest.getBaseAof() != null) {
                loadFile(new File(dir, manifest.getBaseAof().filename));
            }

            // 2. 加载 Incr AOFs (按顺序)
            for (AofManifest.AofInfo info : manifest.getIncrAofs()) {
                loadFile(new File(dir, info.filename));
            }

            log.info("AOF loaded successfully.");

        } catch (IOException e) {
            log.error("Failed to load AOF", e);
            throw new RuntimeException("AOF load failed", e);
        }
    }

    private void loadFile(File file) throws IOException {
        long fileSize = file.length();
        log.info("Start loading AOF file: {} (Size: {} bytes)", file.getName(), fileSize);

        long startTime = System.currentTimeMillis();

        // 读取整个文件到 ByteBuf (注意：如果文件巨大，这里会 OOM)
        // 生产级优化：应该使用 FileChannel 分块读取。
        // Mini-Redis 简化：一次性读取。
        byte[] bytes = Files.readAllBytes(file.toPath());
        ByteBuf buf = Unpooled.wrappedBuffer(bytes);

        // 使用 EmbeddedChannel 复用 RespDecoder，避免重复造轮子
        EmbeddedChannel channel = new EmbeddedChannel(new RespDecoder());

        // 写入数据触发解码
        channel.writeInbound(buf);
        channel.finish();

        int count = 0;
        long lastLogTime = System.currentTimeMillis();

        RedisMessage msg;
        while ((msg = channel.readInbound()) != null) {
            if (msg instanceof RedisArray command) {
                // 执行命令
                // 注意：AOF 重放时 ctx 传 null 是安全的，因为：
                // 1. AOF 只记录写命令 (SET, LPUSH)。
                // 2. 阻塞命令 (BLPOP) 记录的是转换后的非阻塞命令 (LPOP)。
                // 3. Command 实现类必须兼容 ctx 为 null 的情况 (我们的设计已满足)。

                try {
                    dispatcher.dispatch(command, null);
                    count++;

                    // 【进度监控】每 10万 条 或 每 2秒 打印一次进度
                    // 避免大文件加载时控制台“假死”
                    if (count % 100_000 == 0) {
                        long now = System.currentTimeMillis();
                        if (now - lastLogTime > 2000) {
                            log.info("AOF loading progress: {} commands processed...", count);
                            lastLogTime = now;
                        }
                    }
                } catch (Exception e) {
                    log.error("Failed to replay command", e);
                    // 生产环境可能需要策略：忽略错误继续？还是直接退出？
                    // Redis 默认是报错退出的 (aof-load-truncated)，这里我们简单记录日志继续。
                }
            }
        }

        long duration = System.currentTimeMillis() - startTime;
        log.info("AOF file {} loaded successfully. Total commands: {}. Duration: {} ms",
                file.getName(), count, duration);
    }

}
