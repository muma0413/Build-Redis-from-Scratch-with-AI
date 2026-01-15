package org.muma.mini.redis.command.impl.string;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.muma.mini.redis.protocol.*;
import org.muma.mini.redis.server.RedisContext;
import org.muma.mini.redis.store.StorageEngine;
import org.muma.mini.redis.store.impl.MemoryStorageEngine;

import static org.junit.jupiter.api.Assertions.*;

class StringCounterTest {

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
    void testIncrAndDecr() {
        IncrCommand incr = new IncrCommand();
        DecrCommand decr = new DecrCommand();

        // INCR new key -> 1
        assertEquals(1L, ((RedisInteger) incr.execute(storage, args("INCR", "c"), context)).value());
        // INCR -> 2
        assertEquals(2L, ((RedisInteger) incr.execute(storage, args("INCR", "c"), context)).value());
        // DECR -> 1
        assertEquals(1L, ((RedisInteger) decr.execute(storage, args("DECR", "c"), context)).value());
    }

    @Test
    void testIncrByAndDecrBy() {
        IncrByCommand incrBy = new IncrByCommand();
        DecrByCommand decrBy = new DecrByCommand();

        // INCRBY c 10 -> 10
        assertEquals(10L, ((RedisInteger) incrBy.execute(storage, args("INCRBY", "c", "10"), context)).value());
        // DECRBY c 5 -> 5
        assertEquals(5L, ((RedisInteger) decrBy.execute(storage, args("DECRBY", "c", "5"), context)).value());
        // Negative -> 5 + (-2) = 3
        assertEquals(3L, ((RedisInteger) incrBy.execute(storage, args("INCRBY", "c", "-2"), context)).value());
    }

    @Test
    void testErrorOnWrongType() {
        SetCommand set = new SetCommand();
        IncrCommand incr = new IncrCommand();

        // SET k not_a_number
        set.execute(storage, args("SET", "k", "abc"), context);

        // INCR k -> Error
        RedisMessage res = incr.execute(storage, args("INCR", "k"), context);
        assertTrue(res instanceof ErrorMessage);
    }
}
