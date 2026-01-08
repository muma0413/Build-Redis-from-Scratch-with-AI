package org.muma.mini.redis.common;

import org.muma.mini.redis.util.MurmurHash3;

/**
 * 布隆过滤器算法封装
 * 底层操作 byte[]，无状态。
 */
public class BloomFilter {

    // 默认配置
    private static final int DEFAULT_M = 1024 * 8; // 8KB
    private static final int DEFAULT_K = 5;

    /**
     * 计算 K 个 Hash 位置
     */
    public static int[] getHashPositions(byte[] data, int m, int k) {
        int[] positions = new int[k];

        // Hash 1: Murmur3
        int h1 = MurmurHash3.hash32(data);
        // Hash 2: 简单的乘法 Hash (模拟)
        int h2 = simpleHash(data);

        for (int i = 0; i < k; i++) {
            // h = h1 + i * h2
            // Math.abs 保证正数，% m 映射到位图范围
            long combined = h1 + (long) i * h2;
            positions[i] = (int) Math.abs(combined % m);
        }
        return positions;
    }

    private static int simpleHash(byte[] data) {
        int h = 0;
        for (byte b : data) {
            h = 31 * h + b;
        }
        return h;
    }
}
