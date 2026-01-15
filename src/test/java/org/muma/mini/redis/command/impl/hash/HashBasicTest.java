package org.muma.mini.redis.command.impl.hash;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.muma.mini.redis.protocol.*;
import org.muma.mini.redis.server.RedisContext;
import org.muma.mini.redis.store.StorageEngine;
import org.muma.mini.redis.store.impl.MemoryStorageEngine;

import static org.junit.jupiter.api.Assertions.*;

class HashBasicTest {

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
    void testHSetAndHGet() {
        HSetCommand hset = new HSetCommand();
        HGetCommand hget = new HGetCommand();

        // HSET new field -> 1
        RedisMessage res1 = hset.execute(storage, args("HSET", "user", "name", "root"), context);
        assertEquals(1L, ((RedisInteger) res1).value());

        // HSET update field -> 0
        RedisMessage res2 = hset.execute(storage, args("HSET", "user", "name", "admin"), context);
        assertEquals(0L, ((RedisInteger) res2).value());

        // HGET
        assertEquals("admin", asString(hget.execute(storage, args("HGET", "user", "name"), context)));
        // HGET miss
        assertNull(asString(hget.execute(storage, args("HGET", "user", "age"), context)));
    }

    @Test
    void testHMGet() {
        HSetCommand hset = new HSetCommand();
        HMGetCommand hmget = new HMGetCommand();

        hset.execute(storage, args("HSET", "user", "f1", "v1", "f2", "v2"), context);

        RedisArray res = (RedisArray) hmget.execute(storage, args("HMGET", "user", "f1", "f2", "fx"), context);
        assertEquals(3, res.elements().length);
        assertEquals("v1", asString(res.elements()[0]));
        assertEquals("v2", asString(res.elements()[1]));
        assertNull(asString(res.elements()[2]));
    }

    @Test
    void testHDelAndHLenAndHExists() {
        HSetCommand hset = new HSetCommand();
        HDelCommand hdel = new HDelCommand();
        HLenCommand hlen = new HLenCommand();
        HExistsCommand hexists = new HExistsCommand();

        hset.execute(storage, args("HSET", "user", "f1", "v1", "f2", "v2"), context);

        // HLEN
        assertEquals(2L, ((RedisInteger) hlen.execute(storage, args("HLEN", "user"), context)).value());

        // HEXISTS
        assertEquals(1L, ((RedisInteger) hexists.execute(storage, args("HEXISTS", "user", "f1"), context)).value());
        assertEquals(0L, ((RedisInteger) hexists.execute(storage, args("HEXISTS", "user", "fx"), context)).value());

        // HDEL
        assertEquals(1L, ((RedisInteger) hdel.execute(storage, args("HDEL", "user", "f1"), context)).value());
        assertEquals(1L, ((RedisInteger) hlen.execute(storage, args("HLEN", "user"), context)).value());

        // HDEL last element -> Key removed
        hdel.execute(storage, args("HDEL", "user", "f2"), context);
        assertNull(storage.get("user"));
    }
}
