package org.muma.mini.redis.aof;

import org.muma.mini.redis.config.MiniRedisConfig;
import org.muma.mini.redis.protocol.RedisArray;
import org.muma.mini.redis.store.StorageEngine;
import org.muma.mini.redis.utils.RespCodecUtil;
import org.muma.mini.redis.utils.ThreadUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * AOF 核心管理器 (Level 2)
 * 负责 Manifest 维护、文件轮转、Rewrite 状态机调度。
 */
public class AofManager {

    private static final Logger log = LoggerFactory.getLogger(AofManager.class);

    private final MiniRedisConfig config;
    private final AofDiskWriter diskWriter;
    private final AofManifest manifest;
    private final StorageEngine storage; // Rewrite 需要访问内存

    private long lastRewriteTime = 0;
    // Rewrite 专用单线程池
    private final ExecutorService rewriteExecutor = Executors.newSingleThreadExecutor(
            ThreadUtils.namedThreadFactory("AOF-Rewriter")
    );

    // 状态标志位：防止并发 Rewrite
    private final AtomicBoolean isRewriting = new AtomicBoolean(false);

    // 统计数据：用于判断是否触发 Rewrite
    private long lastRewriteSize = 0;
    private long currentAofSize = 0;

    public AofManager(MiniRedisConfig config, StorageEngine storage) {
        this.config = config;
        this.storage = storage; // 需在 Server 启动时注入
        this.diskWriter = new AofDiskWriter(config);
        this.manifest = new AofManifest();
    }

    /**
     * 初始化
     * 必须在 AofLoader.load() 之后调用
     */
    public void init() {
        if (!config.isAppendOnly()) return;

        try {
            loadManifest();

            File aofDir = config.getAofDirFile(); // 【修改】统一获取 AOF 目录

            // 恢复统计数据 (简单取 Base 大小作为初始基准)
            if (manifest.getBaseAof() != null) {
                File baseFile = new File(aofDir, manifest.getBaseAof().filename);
                if (baseFile.exists()) lastRewriteSize = baseFile.length();
            }

            // 检查当前是否有活跃的 Incr 文件
            AofManifest.AofInfo lastIncr = manifest.getLastIncrAof();
            if (lastIncr == null) {
                // 全新启动，创建第一个 Incr
                startNewIncrFile();
            } else {
                // 复用旧的 Incr (Crash 恢复场景)
                diskWriter.open(lastIncr.filename);
                // 恢复 currentAofSize (近似值)
                File incrFile = new File(aofDir, lastIncr.filename);
                if (incrFile.exists()) currentAofSize = incrFile.length();
            }

        } catch (IOException e) {
            throw new RuntimeException("AOF init failed", e);
        }

        // 启动时清理垃圾
        cleanup();
    }

    /**
     * 追加命令 (主线程调用)
     */
    public void append(RedisArray command) {
        if (!config.isAppendOnly()) return;

        try {
            byte[] bytes = RespCodecUtil.encode(command);
            diskWriter.write(bytes);

            // 更新统计并检查 Rewrite
            currentAofSize += bytes.length;
            checkRewrite();

        } catch (Exception e) {
            log.error("Failed to append AOF", e);
        }
    }

    // --- Rewrite 状态机 ---

    private void checkRewrite() {
        if (isRewriting.get()) return;

        // 防抖：距离上次重写至少间隔 1 分钟 (可配置，这里硬编码)
        if (System.currentTimeMillis() - lastRewriteTime < 60 * 1000) return;

        long baseSize = lastRewriteSize == 0 ? 1 : lastRewriteSize;
        long percentage = (currentAofSize * 100) / baseSize;

        if (currentAofSize > config.getAofRewriteMinSize() &&
                percentage >= config.getAofRewritePercentage()) {

            triggerRewrite();
        }
    }

    private void triggerRewrite() {
        if (!isRewriting.compareAndSet(false, true)) return;

        log.info("AOF rewrite triggered. Current Incr Size: {}, Base Size: {}", currentAofSize, lastRewriteSize);

        try {
            // [Phase 1: Pre-Rotate]
            // 在主线程立即切分 Incr 文件，确保新数据写入新的 Incr(N+1)
            // 这样后台 Rewrite 只需关注内存快照，不用管增量同步
            startNewIncrFile();

            // 异步提交任务
            rewriteExecutor.submit(this::performRewrite);

        } catch (Exception e) {
            log.error("Failed to start rewrite", e);
            isRewriting.set(false); // 回滚状态
        }
    }

    /**
     * 清理过期的 AOF 文件
     * 规则：只保留 Manifest 中引用的 Base 和 Incr 文件，其他一律删除。
     */
    private void cleanup() {
        File dir = config.getAofDirFile(); // 【修改】使用 File 对象
        if (!dir.exists()) return;

        // 收集有效文件名
        Set<String> validFiles = new HashSet<>();
        if (manifest.getBaseAof() != null) {
            validFiles.add(manifest.getBaseAof().filename);
        }
        for (AofManifest.AofInfo info : manifest.getIncrAofs()) {
            validFiles.add(info.filename);
        }
        // 还要保留 manifest 文件本身
        validFiles.add("appendonly.aof.manifest");

        File[] files = dir.listFiles((d, name) -> name.startsWith(config.getAppendFilename()) || name.endsWith(".manifest"));

        if (files == null) return;

        // 删除无效文件
        for (File f : files) {
            if (!validFiles.contains(f.getName())) {
                if (f.delete()) {
                    log.info("Deleted orphan AOF file: {}", f.getName());
                } else {
                    log.warn("Failed to delete orphan AOF file: {}", f.getName());
                }
            }
        }
    }


    private void startNewIncrFile() throws IOException {
        long newSeq = manifest.nextSeq();
        String filename = config.getAppendFilename() + "." + newSeq + ".incr.aof";

        // 1. 关闭旧文件 (diskWriter 内部处理)
        diskWriter.open(filename);

        // 2. 更新 Manifest (内存)
        manifest.addIncrAof(filename, newSeq);

        // 3. 持久化 Manifest
        saveManifest();

        // 重置当前增量统计
        currentAofSize = 0;
    }

    /**
     * 执行 AOF 重写的核心逻辑 (运行在 rewriteExecutor 线程中)
     */
    private void performRewrite() {
        long start = System.currentTimeMillis();
        String baseName = null;
        File baseFile = null;

        try {
            // 1. 准备新 Base 文件
            long newSeq = manifest.nextSeq();
            baseName = config.getAppendFilename() + "." + newSeq + ".base.aof";

            File aofDir = config.getAofDirFile(); // 【修改】
            baseFile = new File(aofDir, baseName);

            log.info("Starting AOF rewrite to {}", baseName);

            // 2. 执行快照 (Snapshot & Write)
            AofRewriter rewriter = new AofRewriter(storage);
            rewriter.rewrite(baseFile);

            // 3. 原子切换 (Atomic Switch)
            synchronized (this) {
                // A. 更新 Base 指针
                manifest.setBaseAof(baseName, newSeq);

                // B. 裁剪 Incr 历史
                manifest.pruneHistory();

                // C. 持久化 Manifest 到磁盘
                saveManifest();

                // D. 更新统计状态
                lastRewriteSize = baseFile.length();
                lastRewriteTime = System.currentTimeMillis();
            }

            // 4. 垃圾回收 (Cleanup)
            cleanup();

            long duration = System.currentTimeMillis() - start;
            log.info("AOF rewrite success. New Base: {}, Duration: {} ms", baseName, duration);

        } catch (Exception e) {
            log.error("AOF rewrite failed", e);

            // 失败处理：如果生成了半成品的 Base 文件，尝试删除
            if (baseFile != null && baseFile.exists()) {
                baseFile.delete();
            }
        } finally {
            // 无论成功失败，必须释放锁标志
            isRewriting.set(false);
        }
    }

    // --- 基础辅助 ---

    private void loadManifest() throws IOException {
        File dir = config.getAofDirFile(); // 【修改】
        if (!dir.exists()) dir.mkdirs();
        File f = new File(dir, "appendonly.aof.manifest");

        if (f.exists()) {
            AofManifest loaded = AofManifest.decode(Files.readString(f.toPath()));

            // 【修复点】只有当 loaded 确实有 baseAof 时，才设置
            if (loaded.getBaseAof() != null) {
                this.manifest.setBaseAof(
                        loaded.getBaseAof().filename,
                        loaded.getBaseAof().seq
                );
            }

            // 【修复点】使用新方法清空并添加
            this.manifest.clearIncrAofs();

            for (AofManifest.AofInfo info : loaded.getIncrAofs()) {
                this.manifest.addIncrAof(info.filename, info.seq);
            }
        }
    }

    private void saveManifest() throws IOException {
        File dir = config.getAofDirFile(); // 【修改】
        File temp = new File(dir, "temp.manifest");
        Files.writeString(temp.toPath(), manifest.encode());
        File dest = new File(dir, "appendonly.aof.manifest");

        // 原子 Rename
        try {
            Files.move(temp.toPath(), dest.toPath(), StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            // Windows 可能的 fallback
            if (!temp.renameTo(dest)) {
                throw new IOException("Failed to rename manifest file");
            }
        }
    }

    public void shutdown() {
        if (config.isAppendOnly()) {
            diskWriter.shutdown();
            rewriteExecutor.shutdown();
        }
    }
}
