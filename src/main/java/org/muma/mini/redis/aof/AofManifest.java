package org.muma.mini.redis.aof;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * AOF 清单文件 (Manifest) 管理器
 * 对应 Redis 7.0 的 MP-AOF (Multi-Part AOF) 机制。
 * <p>
 * 职责：
 * 1. 记录当前有效的 Base AOF 文件 (只能有一个)。
 * 2. 记录当前有效的 Incr AOF 文件列表 (有序)。
 * 3. 生成和解析 manifest 文件内容。
 */
public class AofManifest {

    // Base AOF 文件信息 (通常是 RDB 或重写后的 AOF)
    private AofInfo baseAof;

    // Incr AOF 文件列表 (增量日志)
    private final List<AofInfo> incrAofs = new ArrayList<>();

    // 当前全局 Sequence (递增，用于生成新文件名)
    private long currentSeq = 0;

    /**
     * AOF 文件元信息
     */
    public static class AofInfo {
        public String filename;
        public long seq;
        public char type; // 'b' = base, 'i' = incr

        public AofInfo(String filename, long seq, char type) {
            this.filename = filename;
            this.seq = seq;
            this.type = type;
        }

        @Override
        public String toString() {
            // 格式: file appendonly.aof.1.incr.aof seq 1 type i
            return String.format("file %s seq %d type %c", filename, seq, type);
        }
    }

    // --- 核心操作 ---

    /**
     * 获取下一个可用的 Sequence
     */
    public long nextSeq() {
        return ++currentSeq;
    }

    public long getCurrentSeq() {
        return currentSeq;
    }

    public AofInfo getBaseAof() {
        return baseAof;
    }

    public void setBaseAof(String filename, long seq) {
        this.baseAof = new AofInfo(filename, seq, 'b');
        if (seq > currentSeq) currentSeq = seq;
    }

    public List<AofInfo> getIncrAofs() {
        return Collections.unmodifiableList(incrAofs);
    }

    public void addIncrAof(String filename, long seq) {
        this.incrAofs.add(new AofInfo(filename, seq, 'i'));
        if (seq > currentSeq) currentSeq = seq;
    }

    public AofInfo getLastIncrAof() {
        if (incrAofs.isEmpty()) return null;
        return incrAofs.get(incrAofs.size() - 1);
    }

    // --- 序列化与反序列化 ---

    /**
     * 将 Manifest 内容编码为字符串 (用于写入文件)
     */
    public String encode() {
        StringBuilder sb = new StringBuilder();
        if (baseAof != null) {
            sb.append(baseAof.toString()).append("\n");
        }
        for (AofInfo info : incrAofs) {
            sb.append(info.toString()).append("\n");
        }
        return sb.toString();
    }

    /**
     * 从文件内容解析 Manifest
     *
     * @param content manifest 文件的全部内容
     */
    public static AofManifest decode(String content) {
        AofManifest manifest = new AofManifest();
        String[] lines = content.split("\n");

        for (String line : lines) {
            line = line.trim();
            if (line.isEmpty() || line.startsWith("#")) continue;

            // 解析行: file {name} seq {seq} type {type}
            String[] parts = line.split(" ");
            if (parts.length < 6) continue; // 简单容错

            String filename = null;
            long seq = 0;
            char type = 0;

            for (int i = 0; i < parts.length; i++) {
                if ("file".equals(parts[i])) filename = parts[++i];
                else if ("seq".equals(parts[i])) seq = Long.parseLong(parts[++i]);
                else if ("type".equals(parts[i])) type = parts[++i].charAt(0);
            }

            if (type == 'b') {
                manifest.baseAof = new AofInfo(filename, seq, type);
            } else if (type == 'i') {
                manifest.incrAofs.add(new AofInfo(filename, seq, type));
            }

            // 恢复全局 seq
            if (seq > manifest.currentSeq) {
                manifest.currentSeq = seq;
            }
        }
        return manifest;
    }
}
