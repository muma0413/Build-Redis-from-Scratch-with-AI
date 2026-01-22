package org.muma.mini.redis.command.impl.zset;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.muma.mini.redis.common.RedisZSet;
import org.muma.mini.redis.protocol.*;
import org.muma.mini.redis.server.RedisContext;
import org.muma.mini.redis.store.StorageEngine;
import org.muma.mini.redis.store.impl.MemoryStorageEngine;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ZSetStoreTest {

    private StorageEngine storage;
    private RedisContext context;
    private ZAddCommand zadd;

    @BeforeEach
    void setUp() {
        storage = new MemoryStorageEngine();
        context = new RedisContext(null);
        zadd = new ZAddCommand();

        // z1: {a:1, b:2}
        zadd.execute(storage, args("ZADD", "z1", "1", "a", "2", "b"), context);
        // z2: {b:3, c:4}
        zadd.execute(storage, args("ZADD", "z2", "3", "b", "4", "c"), context);
    }

    private RedisArray args(String... args) {
        RedisMessage[] msgs = new RedisMessage[args.length];
        for (int i = 0; i < args.length; i++) msgs[i] = new BulkString(args[i]);
        return new RedisArray(msgs);
    }

    @Test
    void testZUnionStore() {
        ZUnionStoreCommand zunion = new ZUnionStoreCommand();

        // ZUNIONSTORE out 2 z1 z2 WEIGHTS 2 3
        // a: 1*2 = 2
        // b: 2*2 + 3*3 = 13
        // c: 4*3 = 12
        RedisMessage res = zunion.execute(storage, args("ZUNIONSTORE", "out", "2", "z1", "z2", "WEIGHTS", "2", "3"), context);
        assertEquals(3L, ((RedisInteger) res).value());

        RedisZSet out = (RedisZSet) storage.get("out").getData();
        assertEquals(2.0, out.getScore("a"));
        assertEquals(13.0, out.getScore("b"));
        assertEquals(12.0, out.getScore("c"));
    }

    @Test
    void testZInterStore() {
        ZInterStoreCommand zinter = new ZInterStoreCommand();

        // ZINTERSTORE out 2 z1 z2 AGGREGATE MAX
        // Intersect: b
        // b: max(2, 3) = 3
        RedisMessage res = zinter.execute(storage, args("ZINTERSTORE", "out", "2", "z1", "z2", "AGGREGATE", "MAX"), context);
        assertEquals(1L, ((RedisInteger) res).value());

        RedisZSet out = (RedisZSet) storage.get("out").getData();
        assertEquals(3.0, out.getScore("b"));
    }
}
