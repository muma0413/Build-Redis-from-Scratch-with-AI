package org.muma.mini.redis.command.impl.hll;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.muma.mini.redis.protocol.*;
import org.muma.mini.redis.server.RedisContext;
import org.muma.mini.redis.store.StorageEngine;
import org.muma.mini.redis.store.impl.MemoryStorageEngine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class HyperLogLogTest {

    private StorageEngine storage;
    private RedisContext context;
    private PfAddCommand pfAdd;
    private PfCountCommand pfCount;
    private PfMergeCommand pfMerge;

    @BeforeEach
    void setUp() {
        storage = new MemoryStorageEngine();
        context = new RedisContext(null);
        pfAdd = new PfAddCommand();
        pfCount = new PfCountCommand();
        pfMerge = new PfMergeCommand();
    }

    private RedisArray args(String... args) {
        RedisMessage[] msgs = new RedisMessage[args.length];
        for (int i = 0; i < args.length; i++) msgs[i] = new BulkString(args[i]);
        return new RedisArray(msgs);
    }

    @Test
    void testPfAddAndCount() {
        // PFADD hll a b c
        RedisMessage res = pfAdd.execute(storage, args("PFADD", "hll", "a", "b", "c"), context);
        assertEquals(1L, ((RedisInteger) res).value());

        // PFCOUNT hll -> 3
        assertEquals(3L, ((RedisInteger) pfCount.execute(storage, args("PFCOUNT", "hll"), context)).value());

        // PFADD hll a (duplicate)
        res = pfAdd.execute(storage, args("PFADD", "hll", "a"), context);
        assertEquals(0L, ((RedisInteger) res).value()); // 0 means no internal register updated

        // PFCOUNT still 3
        assertEquals(3L, ((RedisInteger) pfCount.execute(storage, args("PFCOUNT", "hll"), context)).value());
    }

    @Test
    void testPfMerge() {
        // h1: {a, b} -> 2
        pfAdd.execute(storage, args("PFADD", "h1", "a", "b"), context);
        // h2: {b, c, d} -> 3
        pfAdd.execute(storage, args("PFADD", "h2", "b", "c", "d"), context);

        // PFMERGE h3 h1 h2
        // Union: {a, b, c, d} -> 4
        RedisMessage res = pfMerge.execute(storage, args("PFMERGE", "h3", "h1", "h2"), context);
        assertEquals("OK", ((SimpleString) res).content());

        // PFCOUNT h3 -> 4
        // 注意：HLL 是估算，M=256 时误差较大。但对于很少的元素(Linear Counting)，通常是准的。
        // 如果这里 assert 4 失败，可以改用 range assert (3~5)
        long count = ((RedisInteger) pfCount.execute(storage, args("PFCOUNT", "h3"), context)).value();
        assertTrue(count >= 3 && count <= 5, "Count should be approx 4, got " + count);
    }

    @Test
    void testPfCountMulti() {
        pfAdd.execute(storage, args("PFADD", "h1", "a"), context);
        pfAdd.execute(storage, args("PFADD", "h2", "b"), context);

        // PFCOUNT h1 h2 -> 2
        long count = ((RedisInteger) pfCount.execute(storage, args("PFCOUNT", "h1", "h2"), context)).value();
        assertEquals(2L, count);
    }
}
