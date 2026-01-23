package org.muma.mini.redis.util;

import java.nio.charset.StandardCharsets;

/**
 * MurmurHash3 x86 32-bit 实现
 * 适合 RedisDict 的 bucket 索引计算
 */
public class MurmurHash3 {

    private static final int C1 = 0xcc9e2d51;
    private static final int C2 = 0x1b873593;
    private static final int SEED = 0; // 可以随机化防止 HashDos 攻击

    public static int hash32(byte[] data) {
        int length = data.length;
        int h1 = SEED;
        int roundedEnd = (length & 0xfffffffc);  // round down to 4 byte block

        for (int i = 0; i < roundedEnd; i += 4) {
            // little-endian load
            int k1 = (data[i] & 0xff) | ((data[i + 1] & 0xff) << 8) | ((data[i + 2] & 0xff) << 16) | (data[i + 3] << 24);
            k1 *= C1;
            k1 = Integer.rotateLeft(k1, 15);
            k1 *= C2;

            h1 ^= k1;
            h1 = Integer.rotateLeft(h1, 13);
            h1 = h1 * 5 + 0xe6546b64;
        }

        // tail
        int k1 = 0;
        switch (length & 0x03) {
            case 3:
                k1 = (data[length - 1] & 0xff) << 16;
            case 2:
                k1 |= (data[length - 2] & 0xff) << 8;
            case 1:
                k1 |= (data[length - 1] & 0xff);
                k1 *= C1;
                k1 = Integer.rotateLeft(k1, 15);
                k1 *= C2;
                h1 ^= k1;
        }

        // finalization
        h1 ^= length;
        h1 ^= (h1 >>> 16);
        h1 *= 0x85ebca6b;
        h1 ^= (h1 >>> 13);
        h1 *= 0xc2b2ae35;
        h1 ^= (h1 >>> 16);

        return h1;
    }


    // 简单的 64 位 MurmurHash3 实现 (或者使用 Guava/Commons 的实现)
    // 这里为了不引入依赖，我们使用一个简单的组合策略：
    // hash64 = (long)hash32(data) | ((long)hash32(reverse(data)) << 32)
    // 虽然不是标准的 Murmur64，但对于 HLL 演示足够随机了。
    public static long hash64(byte[] data) {
        int h1 = hash32(data);
        // 反转数组计算第二个 hash (模拟独立性)
        byte[] rev = new byte[data.length];
        for (int i = 0; i < data.length; i++) rev[i] = data[data.length - 1 - i];
        int h2 = hash32(rev);
        return ((long) h1 << 32) | (h2 & 0xFFFFFFFFL);
    }

    // 适配 String
    public static int hash32(String str) {
        return hash32(str.getBytes(StandardCharsets.UTF_8));
    }
}
