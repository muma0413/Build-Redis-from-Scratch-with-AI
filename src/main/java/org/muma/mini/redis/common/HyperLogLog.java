package org.muma.mini.redis.common;

import org.muma.mini.redis.util.MurmurHash3;

/**
 * HyperLogLog 算法实现 (简化版)
 * p = 8 (256 buckets)
 */
public class HyperLogLog {

    private static final int P = 8; // 桶位数
    private static final int M = 1 << P; // 256 桶
    private static final double ALPHA = 0.7213 / (1 + 1.079 / M); // 修正因子

    /**
     * 添加元素
     *
     * @param registers 当前的寄存器数组 (byte[])
     * @param element   元素
     * @return true if registers updated
     */
    public static boolean add(byte[] registers, byte[] element) {
        if (registers.length != M) throw new IllegalArgumentException("Invalid HLL size");

        long hash = MurmurHash3.hash64(element);

        // 1. 计算桶索引 (取低 P 位)
        int index = (int) (hash & (M - 1));

        // 2. 计算剩余位的前导零 (Leading Zeros) + 1
        long w = hash >>> P;
        int count = Long.numberOfLeadingZeros(w) + 1; // 1..57 (因为 w 是 64-8=56位)

        // 3. 更新寄存器 (保留最大值)
        if (count > registers[index]) {
            registers[index] = (byte) count;
            return true;
        }
        return false;
    }

    /**
     * 计算基数 (Count)
     */
    public static long count(byte[] registers) {
        double sum = 0;
        int zeros = 0;

        // 调和平均数公式: sum(2^-reg[j])
        for (byte reg : registers) {
            if (reg == 0) zeros++;
            sum += Math.pow(2, -reg);
        }

        double estimate = ALPHA * M * M / sum;

        // 小范围修正 (Linear Counting)
        if (estimate <= 2.5 * M) {
            if (zeros > 0) {
                estimate = M * Math.log((double) M / zeros);
            }
        }

        return (long) estimate;
    }

    // 合并 (PFMERGE)
    public static void merge(byte[] dest, byte[] src) {
        for (int i = 0; i < M; i++) {
            if (src[i] > dest[i]) {
                dest[i] = src[i];
            }
        }
    }
}
