package org.muma.mini.redis.store.structure;

import org.muma.mini.redis.common.RedisZSet;
import org.muma.mini.redis.store.structure.impl.zset.RangeSpec;

import java.util.List;

/**
 *
 */
public interface ZSetProvider {

    int add(double score, String member);

    int remove(String member);

    Double getScore(String member);

    Long getRank(String member);

    List<RedisZSet.ZSetEntry> range(long start, long stop);

    int size();

    List<RedisZSet.ZSetEntry> getAll();

    List<RedisZSet.ZSetEntry> revRange(long start, long stop);

    List<RedisZSet.ZSetEntry> rangeByScore(RangeSpec range, int offset, int count);

    long count(RangeSpec range);

}
