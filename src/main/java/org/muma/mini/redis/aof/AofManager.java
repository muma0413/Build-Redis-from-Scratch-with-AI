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
import java.util.ArrayList;
import java.util.List;
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

            // 恢复统计数据 (简单取 Base 大小作为初始基准)
            if (manifest.getBaseAof() != null) {
                File baseFile = new File(config.getAppendDir(), manifest.getBaseAof().filename);
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
                File incrFile = new File(config.getAppendDir(), lastIncr.filename);
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

            // 记录下此时应该被废弃的文件 (即刚才切分前的 Incr(N) 和旧 Base)
            // 但为了安全，我们不在这里删，等 Rewrite 成功后再删。

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
        File dir = new File(config.getAppendDir());
        File[] files = dir.listFiles((d, name) -> name.startsWith(config.getAppendFilename()) && name.endsWith(".aof"));

        if (files == null) return;

        // 收集有效文件名
        java.util.Set<String> validFiles = new java.util.HashSet<>();
        if (manifest.getBaseAof() != null) {
            validFiles.add(manifest.getBaseAof().filename);
        }
        for (AofManifest.AofInfo info : manifest.getIncrAofs()) {
            validFiles.add(info.filename);
        }

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

        // 1. 关闭旧文件
        // diskWriter 内部会 flush 并 close channel
        // 但注意：diskWriter 是单例，我们只是让它 reopen
        // open 方法内部会先 close 旧的
        diskWriter.open(filename);

        // 2. 更新 Manifest (内存)
        manifest.addIncrAof(filename, newSeq);

        // 3. 持久化 Manifest
        saveManifest();

        // 重置当前增量统计
        currentAofSize = 0;
    }

    /**
     * 后台重写逻辑
     */
    /**
     * 执行 AOF 重写的核心逻辑 (运行在 rewriteExecutor 线程中)
     * <p>
     * 流程：
     * 1. 准备新 Base 文件。
     * 2. 执行快照：遍历内存生成指令，写入新 Base。
     * 3. 原子切换：更新 Manifest，指向新 Base，废弃旧 Incr。
     * 4. 垃圾回收：清理旧文件。
     */
    private void performRewrite() {
        // 记录开始时间
        long start = System.currentTimeMillis();
        String baseName = null;
        File baseFile = null;

        try {
            // 1. 准备新 Base 文件
            // 使用独立的 Sequence 生成文件名，避免与 Incr 冲突
            long newSeq = manifest.nextSeq();
            baseName = config.getAppendFilename() + "." + newSeq + ".base.aof";
            baseFile = new File(config.getAppendDir(), baseName);

            log.info("Starting AOF rewrite to {}", baseName);

            // 2. 执行快照 (Snapshot & Write)
            // 这一步最耗时，但不会阻塞主线程 (依赖 ConcurrentHashMap 的弱一致性迭代器)
            AofRewriter rewriter = new AofRewriter(storage);
            rewriter.rewrite(baseFile);

            // 3. 原子切换 (Atomic Switch)
            // 必须加锁，防止与主线程的 append/triggerRewrite 冲突
            synchronized (this) {
                // A. 更新 Base 指针
                manifest.setBaseAof(baseName, newSeq);

                // B. 裁剪 Incr 历史
                // 此时 Manifest 里的 Incr 列表大概是 [Incr(Old), Incr(New)]
                // Incr(Old) 的数据已经被重写进 Base 了，可以丢弃。
                // Incr(New) 是重写期间产生的新数据，必须保留。
                // pruneHistory() 会只保留列表中最后一个 Incr。
                manifest.pruneHistory();

                // C. 持久化 Manifest 到磁盘
                // 这一步成功后，新的 Base 正式生效。
                saveManifest();

                // D. 更新统计状态
                lastRewriteSize = baseFile.length();
                lastRewriteTime = System.currentTimeMillis();

                // 重置当前增量大小 (因为旧的 Incr 被归档进 Base 了，现在的 Incr 是新开的)
                // 注意：这里 currentAofSize 应该重置为当前活跃 Incr 文件的大小
                // 但由于我们在 triggerRewrite 时已经切换了新 Incr 并重置为 0，
                // 且重写期间主线程一直在累加它，所以这里不需要清零，保持原样即可。
            }

            // 4. 垃圾回收 (Cleanup)
            // 删除那些不再被 Manifest 引用的旧 Base 和旧 Incr
            // 异步执行，不占用锁
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


    private void deleteOldFiles(List<AofManifest.AofInfo> files) {
        for (AofManifest.AofInfo info : files) {
            File f = new File(config.getAppendDir(), info.filename);
            if (f.exists()) f.delete();
        }
        // 也要删旧的 Base (如果有) -> 这个逻辑需要 manifest 维护历史 base 列表或者每次 setBase 时记录旧的
        // 简单实现：Manifest setBaseAof 时没返回旧的。
        // 可以在 setBaseAof 前 manually check。
    }

    // --- 基础辅助 ---

    private void loadManifest() throws IOException {
        File dir = new File(config.getAppendDir());
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
            this.manifest.clearIncrAofs(); // 不再直接调 getIncrAofs().clear()

            for (AofManifest.AofInfo info : loaded.getIncrAofs()) {
                this.manifest.addIncrAof(info.filename, info.seq);
            }
        }
    }

    private void saveManifest() throws IOException {
        File dir = new File(config.getAppendDir());
        File temp = new File(dir, "temp.manifest");
        Files.writeString(temp.toPath(), manifest.encode());
        File dest = new File(dir, "appendonly.aof.manifest");
        // 原子 Rename
        if (!temp.renameTo(dest)) {
            // Windows 下可能失败如果 dest 存在，需先删后挪，或者用 Files.move ATOMIC_MOVE
            Files.move(temp.toPath(), dest.toPath(), java.nio.file.StandardCopyOption.REPLACE_EXISTING);
        }
    }

    public void shutdown() {
        if (config.isAppendOnly()) {
            diskWriter.shutdown();
            rewriteExecutor.shutdown();
        }
    }
}
