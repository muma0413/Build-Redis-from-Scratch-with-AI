package org.muma.mini.redis.rdb;

import java.nio.charset.StandardCharsets;

public class RdbConstants {

    // Header: REDIS0009
    public static final byte[] MAGIC = "REDIS".getBytes(StandardCharsets.UTF_8);
    public static final String VERSION = "0009";

    // --- OpCodes (操作码) ---

    // 标识数据库选择 (SELECT DB)
    public static final int OP_SELECTDB = 0xFE; // 254

    // 标识过期时间 (毫秒, 8 bytes)
    public static final int OP_EXPIRETIME_MS = 0xFC; // 252

    // 标识过期时间 (秒, 4 bytes, 旧版本用, 这里备用)
    public static final int OP_EXPIRETIME = 0xFD; // 253

    // 标识 RDB 文件结束
    public static final int OP_EOF = 0xFF; // 255

    // 标识调整哈希表大小 (Resize DB)
    public static final int OP_RESIZEDB = 0xFB; // 251

    // 标识辅助字段 (Auxiliary field)
    public static final int OP_AUX = 0xFA; // 250

    private RdbConstants() {
    }
}
