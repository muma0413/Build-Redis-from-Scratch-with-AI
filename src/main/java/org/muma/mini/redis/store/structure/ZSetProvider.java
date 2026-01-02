package org.muma.mini.redis.store.structure;

import org.muma.mini.redis.common.RedisZSet;

import java.util.List;

public interface ZSetProvider {
    int add(double score, String member);

    int remove(String member);

    Double getScore(String member);

    Long getRank(String member);

    List<RedisZSet.ZSetEntry> range(long start, long stop);

    int size();

    List<RedisZSet.ZSetEntry> getAll();
}
