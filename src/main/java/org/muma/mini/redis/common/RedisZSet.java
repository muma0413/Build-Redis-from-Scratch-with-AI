package org.muma.mini.redis.common;

import org.muma.mini.redis.store.structure.ZSetProvider;
import org.muma.mini.redis.store.structure.impl.zset.RangeSpec;
import org.muma.mini.redis.store.structure.impl.zset.SkipListZSetProvider;
import org.muma.mini.redis.store.structure.impl.zset.ZipListZSetProvider;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Redis ZSet 核心封装类
 * 负责管理底层存储引擎 (ZipList vs SkipList) 的切换
 */
public class RedisZSet implements Serializable {

    // 数据传输对象 (DTO)
    public record ZSetEntry(String member, double score) {}

    // Redis 默认阈值配置
    private static final int MAX_ZIPLIST_ENTRIES = 128;
    private static final int MAX_ZIPLIST_VALUE = 64;

    // 核心存储引擎接口
    private ZSetProvider provider;

    public RedisZSet() {
        // 默认初始化为轻量级 ZipList
        this.provider = new ZipListZSetProvider();
    }

    /**
     * 添加元素
     */
    public int add(double score, String member) {
        // 1. 检查是否需要因 Value 过长而升级
        if (isZipList() && member.getBytes(StandardCharsets.UTF_8).length > MAX_ZIPLIST_VALUE) {
            upgrade();
        }

        int res = provider.add(score, member);

        // 2. 检查是否需要因数量过多而升级
        if (isZipList() && provider.size() > MAX_ZIPLIST_ENTRIES) {
            upgrade();
        }
        return res;
    }

    /**
     * 删除元素
     */
    public int remove(String member) {
        return provider.remove(member);
    }

    // --- 【本次补全的核心方法】 ---

    /**
     * 获取分数
     */
    public Double getScore(String member) {
        return provider.getScore(member);
    }

    /**
     * 获取排名
     */
    public Long getRank(String member) {
        return provider.getRank(member);
    }

    // ---------------------------

    /**
     * 范围查询
     */
    public List<ZSetEntry> range(long start, long stop) {
        return provider.range(start, stop);
    }

    /**
     * 获取大小
     */
    public int size() {
        return provider.size();
    }

    // --- 内部私有方法 ---

    private boolean isZipList() {
        return provider instanceof ZipListZSetProvider;
    }

    /**
     * 核心升级逻辑：ZipList -> SkipList
     */
    private void upgrade() {
        // System.out.println("DEBUG: Upgrading ZSet from ZipList to SkipList...");
        List<ZSetEntry> allData = provider.getAll();
        SkipListZSetProvider newProvider = new SkipListZSetProvider();

        for (ZSetEntry entry : allData) {
            newProvider.add(entry.score(), entry.member());
        }
        this.provider = newProvider;
    }

    /**
     * 自增辅助方法 (可选，供 ZINCRBY 使用)
     */
    public double incrBy(double increment, String member) {
        Double old = getScore(member);
        double val = (old == null ? 0 : old) + increment;
        add(val, member);
        return val;
    }

    /**
     * 反向范围查询 (ZREVRANGE)
     * @param start 0-based start index
     * @param stop 0-based stop index
     */
    public List<ZSetEntry> revRange(long start, long stop) {
        return provider.revRange(start, stop);
    }


    /**
     * 按分数范围查询 (ZRANGEBYSCORE)
     * @param range 分数范围定义
     * @param offset LIMIT offset
     * @param count LIMIT count
     */
    public List<ZSetEntry> rangeByScore(RangeSpec range, int offset, int count) {
        return provider.rangeByScore(range, offset, count);
    }


    /**
     * 按分数范围计数 (ZCOUNT)
     * @param range 分数范围定义
     */
    public long count(RangeSpec range) {
        return provider.count(range);
    }
}
