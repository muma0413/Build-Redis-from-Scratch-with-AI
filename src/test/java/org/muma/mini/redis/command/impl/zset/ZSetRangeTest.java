package org.muma.mini.redis.command.impl.zset;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.muma.mini.redis.protocol.*;
import org.muma.mini.redis.server.RedisContext;
import org.muma.mini.redis.store.StorageEngine;
import org.muma.mini.redis.store.impl.MemoryStorageEngine;

import static org.junit.jupiter.api.Assertions.*;

class ZSetRangeTest {

    private StorageEngine storage;
    private RedisContext context;
    private ZAddCommand zadd;

    @BeforeEach
    void setUp() {
        storage = new MemoryStorageEngine();
        context = new RedisContext(null);
        zadd = new ZAddCommand();
        // {a:1, b:2, c:3, d:4, e:5}
        zadd.execute(storage, args("ZADD", "k", "1", "a", "2", "b", "3", "c", "4", "d", "5", "e"), context);
    }

    private RedisArray args(String... args) {
        RedisMessage[] msgs = new RedisMessage[args.length];
        for (int i = 0; i < args.length; i++) msgs[i] = new BulkString(args[i]);
        return new RedisArray(msgs);
    }

    @Test
    void testZRange() {
        ZRangeCommand zrange = new ZRangeCommand();

        // ZRANGE k 0 1 -> a, b
        RedisArray res = (RedisArray) zrange.execute(storage, args("ZRANGE", "k", "0", "1"), context);
        assertEquals(2, res.elements().length);
        assertEquals("a", ((BulkString)res.elements()[0]).asString());

        // WITHSCORES
        res = (RedisArray) zrange.execute(storage, args("ZRANGE", "k", "0", "0", "WITHSCORES"), context);
        assertEquals(2, res.elements().length);
        assertEquals("a", ((BulkString)res.elements()[0]).asString());
        assertEquals("1", ((BulkString)res.elements()[1]).asString());
    }

    @Test
    void testZRevRange() {
        ZRevRangeCommand zrevrange = new ZRevRangeCommand();

        // ZREVRANGE k 0 1 -> e, d
        RedisArray res = (RedisArray) zrevrange.execute(storage, args("ZREVRANGE", "k", "0", "1"), context);
        assertEquals(2, res.elements().length);
        assertEquals("e", ((BulkString)res.elements()[0]).asString());
        assertEquals("d", ((BulkString)res.elements()[1]).asString());
    }

    @Test
    void testZRangeByScore() {
        ZRangeByScoreCommand zrangebyscore = new ZRangeByScoreCommand();

        // 2 <= score <= 4 -> b, c, d
        RedisArray res = (RedisArray) zrangebyscore.execute(storage, args("ZRANGEBYSCORE", "k", "2", "4"), context);
        assertEquals(3, res.elements().length);

        // LIMIT 1 1 -> c (skip b, take 1)
        res = (RedisArray) zrangebyscore.execute(storage, args("ZRANGEBYSCORE", "k", "2", "4", "LIMIT", "1", "1"), context);
        assertEquals(1, res.elements().length);
        assertEquals("c", ((BulkString)res.elements()[0]).asString());
    }

    @Test
    void testZRemRange() {
        ZRemRangeByScoreCommand zremscore = new ZRemRangeByScoreCommand();
        ZRemRangeByRankCommand zremrank = new ZRemRangeByRankCommand();

        // REM SCORE 1 2 -> {c, d, e} left
        assertEquals(2L, ((RedisInteger) zremscore.execute(storage, args("ZREMRANGEBYSCORE", "k", "1", "2"), context)).value());

        // REM RANK 0 0 -> remove c (new rank 0) -> {d, e} left
        assertEquals(1L, ((RedisInteger) zremrank.execute(storage, args("ZREMRANGEBYRANK", "k", "0", "0"), context)).value());

        // Verify left: d, e
        assertEquals(2, ((org.muma.mini.redis.common.RedisZSet)storage.get("k").getData()).size());
    }
}
