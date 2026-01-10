package org.muma.mini.redis.replication;

import lombok.Getter;
import lombok.Setter;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 复制元数据
 * Master 和 Slave 都需要维护
 */
public class ReplicationMetadata {

    // 自身 RunID (作为 Master 时用)
    @Getter
    private final String myRunId;

    // 全局复制偏移量 (Master: 写入量; Slave: 已同步量)
    private final AtomicLong replOffset = new AtomicLong(0);

    // Master 的 Host/Port (仅 Slave 模式有效)
    @Getter
    private String masterHost;
    @Getter
    private int masterPort = -1;

    // 缓存的 Master RunID (Slave 模式下记录)
    @Setter
    @Getter
    private String cachedMasterRunId = "?";

    public ReplicationMetadata() {
        // 生成 40 字节十六进制 ID (简单起见用 UUID 去掉横杠)
        this.myRunId = UUID.randomUUID().toString().replace("-", "") + UUID.randomUUID().toString().substring(0, 8);
    }

    public long getReplOffset() {
        return replOffset.get();
    }

    public void addOffset(long delta) {
        replOffset.addAndGet(delta);
    }

    public void setReplOffset(long offset) {
        replOffset.set(offset);
    }

    public void setMaster(String host, int port) {
        this.masterHost = host;
        this.masterPort = port;
    }

    public void clearMaster() {
        this.masterHost = null;
        this.masterPort = -1;
        this.cachedMasterRunId = "?";
    }

}
