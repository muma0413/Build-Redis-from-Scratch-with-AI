package org.muma.mini.redis.command.impl.string;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.muma.mini.redis.command.impl.key.TTLCommand;
import org.muma.mini.redis.protocol.*;
import org.muma.mini.redis.server.RedisContext;
import org.muma.mini.redis.store.StorageEngine;
import org.muma.mini.redis.store.impl.MemoryStorageEngine;

import static org.junit.jupiter.api.Assertions.*;

class StringBasicTest {

    private StorageEngine storage;
    private RedisContext context; // Mock context if needed

    @BeforeEach
    void setUp() {
        storage = new MemoryStorageEngine();
        context = new RedisContext(null); // String commands usually don't need ctx
    }

    private RedisArray args(String... args) {
        RedisMessage[] msgs = new RedisMessage[args.length];
        for (int i = 0; i < args.length; i++) msgs[i] = new BulkString(args[i]);
        return new RedisArray(msgs);
    }

    private String asString(RedisMessage msg) {
        if (msg instanceof BulkString b) return b.asString();
        if (msg instanceof SimpleString s) return s.content();
        return null;
    }

    @Test
    void testSetAndGet() {
        SetCommand set = new SetCommand();
        GetCommand get = new GetCommand();

        // SET key val
        assertEquals("OK", asString(set.execute(storage, args("SET", "k1", "v1"), context)));
        // GET key
        assertEquals("v1", asString(get.execute(storage, args("GET", "k1"), context)));
        // GET non-exist
        assertNull(asString(get.execute(storage, args("GET", "nx"), context)));
    }

    @Test
    void testSetNx() {
        SetNxCommand setnx = new SetNxCommand();
        // 1. First set -> 1
        RedisMessage res1 = setnx.execute(storage, args("SETNX", "k1", "v1"), context);
        assertEquals(1L, ((RedisInteger) res1).value());

        // 2. Second set -> 0
        RedisMessage res2 = setnx.execute(storage, args("SETNX", "k1", "v2"), context);
        assertEquals(0L, ((RedisInteger) res2).value());
    }

    @Test
    void testMSetAndMGet() {
        MSetCommand mset = new MSetCommand();
        MGetCommand mget = new MGetCommand();

        mset.execute(storage, args("MSET", "k1", "v1", "k2", "v2"), context);

        RedisArray res = (RedisArray) mget.execute(storage, args("MGET", "k1", "k2", "nx"), context);
        assertEquals(3, res.elements().length);
        assertEquals("v1", asString(res.elements()[0]));
        assertEquals("v2", asString(res.elements()[1]));
        assertNull(asString(res.elements()[2]));
    }

    @Test
    void testAppendAndStrLen() {
        AppendCommand append = new AppendCommand();
        StrLenCommand strlen = new StrLenCommand();

        // Append to new key
        RedisMessage res1 = append.execute(storage, args("APPEND", "k1", "hello"), context);
        assertEquals(5L, ((RedisInteger) res1).value());

        // Append again
        RedisMessage res2 = append.execute(storage, args("APPEND", "k1", " world"), context);
        assertEquals(11L, ((RedisInteger) res2).value());

        // StrLen
        RedisMessage len = strlen.execute(storage, args("STRLEN", "k1"), context);
        assertEquals(11L, ((RedisInteger) len).value());

        // StrLen non-exist
        assertEquals(0L, ((RedisInteger) strlen.execute(storage, args("STRLEN", "nx"), context)).value());
    }

    @Test
    void testGetEx() {
        GetExCommand getex = new GetExCommand();
        SetCommand set = new SetCommand();
        TTLCommand ttl = new TTLCommand(); // Need this to verify

        set.execute(storage, args("SET", "k1", "v1"), context);

        // GETEX k1 EX 10
        RedisMessage res = getex.execute(storage, args("GETEX", "k1", "EX", "10"), context);
        assertEquals("v1", asString(res));

        // Verify TTL > 0
        RedisInteger ttlVal = (RedisInteger) ttl.execute(storage, args("TTL", "k1"), context);
        assertTrue(ttlVal.value() > 0);

        // GETEX k1 PERSIST
        getex.execute(storage, args("GETEX", "k1", "PERSIST"), context);
        ttlVal = (RedisInteger) ttl.execute(storage, args("TTL", "k1"), context);
        assertEquals(-1L, ttlVal.value());
    }
}
