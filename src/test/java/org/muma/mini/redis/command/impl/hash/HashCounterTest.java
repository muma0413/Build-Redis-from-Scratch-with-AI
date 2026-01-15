package org.muma.mini.redis.command.impl.hash;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.muma.mini.redis.protocol.*;
import org.muma.mini.redis.server.RedisContext;
import org.muma.mini.redis.store.StorageEngine;
import org.muma.mini.redis.store.impl.MemoryStorageEngine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class HashCounterTest {

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

    @Test
    void testHIncrBy() {
        HIncrByCommand hincrby = new HIncrByCommand();

        // 1. New field
        RedisMessage res1 = hincrby.execute(storage, args("HINCRBY", "stats", "view", "1"), context);
        assertEquals(1L, ((RedisInteger) res1).value());

        // 2. Existing field
        RedisMessage res2 = hincrby.execute(storage, args("HINCRBY", "stats", "view", "5"), context);
        assertEquals(6L, ((RedisInteger) res2).value());

        // 3. Negative
        RedisMessage res3 = hincrby.execute(storage, args("HINCRBY", "stats", "view", "-2"), context);
        assertEquals(4L, ((RedisInteger) res3).value());
    }

    @Test
    void testHIncrByError() {
        HSetCommand hset = new HSetCommand();
        HIncrByCommand hincrby = new HIncrByCommand();

        hset.execute(storage, args("HSET", "stats", "title", "hello"), context);

        // HINCRBY on non-integer
        RedisMessage err = hincrby.execute(storage, args("HINCRBY", "stats", "title", "1"), context);
        assertTrue(err instanceof ErrorMessage);
    }
}
