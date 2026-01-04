package org.muma.mini.redis.store.structure.impl.zset;

public class RangeSpec {

    public double min, max;
    public boolean minex, maxex; // 是否排除边界 (exclusive)

    public RangeSpec(double min, double max, boolean minex, boolean maxex) {
        this.min = min;
        this.max = max;
        this.minex = minex;
        this.maxex = maxex;
    }

    // 判断值是否在范围内
    public boolean contains(double score) {
        if (minex ? score <= min : score < min) return false;
        if (maxex ? score >= max : score > max) return false;
        return true;
    }

    /**
     * 静态工厂方法：解析 Redis 风格的 min/max 字符串
     */
    public static RangeSpec parse(String minStr, String maxStr) {
        double min, max;
        boolean minex = false, maxex = false;

        // 解析 min
        if ("-inf".equals(minStr)) min = Double.NEGATIVE_INFINITY;
        else if ("+inf".equals(minStr)) min = Double.POSITIVE_INFINITY;
        else {
            if (minStr.startsWith("(")) {
                minex = true;
                minStr = minStr.substring(1);
            }
            try {
                min = Double.parseDouble(minStr);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("min invalid");
            }
        }

        // 解析 max
        if ("+inf".equals(maxStr)) max = Double.POSITIVE_INFINITY;
        else if ("-inf".equals(maxStr)) max = Double.NEGATIVE_INFINITY;
        else {
            if (maxStr.startsWith("(")) {
                maxex = true;
                maxStr = maxStr.substring(1);
            }
            try {
                max = Double.parseDouble(maxStr);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("max invalid");
            }
        }

        return new RangeSpec(min, max, minex, maxex);
    }
}
