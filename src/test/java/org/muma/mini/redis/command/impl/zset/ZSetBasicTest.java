package org.muma.mini.redis.command.impl.zset;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.muma.mini.redis.protocol.*;
import org.muma.mini.redis.server.RedisContext;
import org.muma.mini.redis.store.StorageEngine;
import org.muma.mini.redis.store.impl.MemoryStorageEngine;

import static org.junit.jupiter.api.Assertions.*;

class ZSetBasicTest {

    private StorageEngine storage;
    private RedisContext context;

    @BeforeEach
    void setUp() {
        storage = new MemoryStorageEngine();
        context = new RedisContext(null);
    }

    private RedisArray args(String... args) {
        RedisMessage[] msgs = new RedisMessage[args.length];
        for (int i = 0; i < args.length; i++) msgs[i] = new BulkString(args[i]);
        return new RedisArray(msgs);
    }

    private String asString(RedisMessage msg) {
        if (msg instanceof BulkString b) return b.asString();
        return null;
    }

    @Test
    void testZAddAndZScore() {
        ZAddCommand zadd = new ZAddCommand();
        ZScoreCommand zscore = new ZScoreCommand();

        // ZADD k 10 m1 20 m2
        assertEquals(2L, ((RedisInteger) zadd.execute(storage, args("ZADD", "k", "10", "m1", "20", "m2"), context)).value());

        // ZSCORE k m1 -> 10
        assertEquals("10", asString(zscore.execute(storage, args("ZSCORE", "k", "m1"), context)));

        // ZADD update
        assertEquals(0L, ((RedisInteger) zadd.execute(storage, args("ZADD", "k", "30", "m1"), context)).value());
        assertEquals("30", asString(zscore.execute(storage, args("ZSCORE", "k", "m1"), context)));
    }

    @Test
    void testZIncrBy() {
        ZAddCommand zadd = new ZAddCommand();
        ZIncrByCommand zincrby = new ZIncrByCommand();

        zadd.execute(storage, args("ZADD", "k", "10", "m1"), context);

        // ZINCRBY k 5 m1 -> 15
        assertEquals("15", asString(zincrby.execute(storage, args("ZINCRBY", "k", "5", "m1"), context)));

        // New member -> 5
        assertEquals("5", asString(zincrby.execute(storage, args("ZINCRBY", "k", "5", "new"), context)));
    }

    @Test
    void testZCount() {
        ZAddCommand zadd = new ZAddCommand();
        ZCountCommand zcount = new ZCountCommand();

        zadd.execute(storage, args("ZADD", "k", "10", "a", "20", "b", "30", "c"), context);

        // ZCOUNT k 10 20 -> 2
        assertEquals(2L, ((RedisInteger) zcount.execute(storage, args("ZCOUNT", "k", "10", "20"), context)).value());

        // ZCOUNT k (10 30 -> 2 (20, 30)
        assertEquals(2L, ((RedisInteger) zcount.execute(storage, args("ZCOUNT", "k", "(10", "30"), context)).value());

        // ZCOUNT k -inf +inf -> 3
        assertEquals(3L, ((RedisInteger) zcount.execute(storage, args("ZCOUNT", "k", "-inf", "+inf"), context)).value());
    }
}
