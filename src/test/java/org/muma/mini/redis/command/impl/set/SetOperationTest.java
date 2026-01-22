package org.muma.mini.redis.command.impl.set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.muma.mini.redis.protocol.BulkString;
import org.muma.mini.redis.protocol.RedisArray;
import org.muma.mini.redis.protocol.RedisInteger;
import org.muma.mini.redis.protocol.RedisMessage;
import org.muma.mini.redis.server.RedisContext;
import org.muma.mini.redis.store.StorageEngine;
import org.muma.mini.redis.store.impl.MemoryStorageEngine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SetOperationTest {

    private StorageEngine storage;
    private RedisContext context;
    private SAddCommand sadd;

    @BeforeEach
    void setUp() {
        storage = new MemoryStorageEngine();
        context = new RedisContext(null);
        sadd = new SAddCommand();

        // Set1: {a, b, c}
        sadd.execute(storage, args("SADD", "s1", "a", "b", "c"), context);
        // Set2: {b, c, d}
        sadd.execute(storage, args("SADD", "s2", "b", "c", "d"), context);
        // Set3: {c, d, e}
        sadd.execute(storage, args("SADD", "s3", "c", "d", "e"), context);
    }

    private RedisArray args(String... args) {
        RedisMessage[] msgs = new RedisMessage[args.length];
        for (int i = 0; i < args.length; i++) msgs[i] = new BulkString(args[i]);
        return new RedisArray(msgs);
    }

    @Test
    void testSInter() {
        SInterCommand sinter = new SInterCommand();

        // s1 ∩ s2 = {b, c}
        RedisArray res = (RedisArray) sinter.execute(storage, args("SINTER", "s1", "s2"), context);
        assertEquals(2, res.elements().length);
        assertTrue(hasMember(res, "b"));
        assertTrue(hasMember(res, "c"));

        // s1 ∩ s2 ∩ s3 = {c}
        res = (RedisArray) sinter.execute(storage, args("SINTER", "s1", "s2", "s3"), context);
        assertEquals(1, res.elements().length);
        assertTrue(hasMember(res, "c"));
    }

    @Test
    void testSUnion() {
        SUnionCommand sunion = new SUnionCommand();

        // s1 U s2 = {a, b, c, d}
        RedisArray res = (RedisArray) sunion.execute(storage, args("SUNION", "s1", "s2"), context);
        assertEquals(4, res.elements().length);
    }

    @Test
    void testSDiff() {
        SDiffCommand sdiff = new SDiffCommand();

        // s1 - s2 = {a} (b,c 在 s2 中有)
        RedisArray res = (RedisArray) sdiff.execute(storage, args("SDIFF", "s1", "s2"), context);
        assertEquals(1, res.elements().length);
        assertTrue(hasMember(res, "a"));

        // s2 - s1 = {d}
        res = (RedisArray) sdiff.execute(storage, args("SDIFF", "s2", "s1"), context);
        assertEquals(1, res.elements().length);
        assertTrue(hasMember(res, "d"));
    }

    @Test
    void testSInterCard() {
        SInterCardCommand scard = new SInterCardCommand();

        // s1 ∩ s2 = {b, c} -> 2
        assertEquals(2L, ((RedisInteger) scard.execute(storage, args("SINTERCARD", "2", "s1", "s2"), context)).value());

        // Limit 1
        assertEquals(1L, ((RedisInteger) scard.execute(storage, args("SINTERCARD", "2", "s1", "s2", "LIMIT", "1"), context)).value());
    }

    @Test
    void testSMove() {
        SMoveCommand smove = new SMoveCommand();
        SIsMemberCommand sismember = new SIsMemberCommand();

        // Move 'a' from s1 to s2
        assertEquals(1L, ((RedisInteger) smove.execute(storage, args("SMOVE", "s1", "s2", "a"), context)).value());

        // Verify s1 has no 'a', s2 has 'a'
        assertEquals(0L, ((RedisInteger) sismember.execute(storage, args("SISMEMBER", "s1", "a"), context)).value());
        assertEquals(1L, ((RedisInteger) sismember.execute(storage, args("SISMEMBER", "s2", "a"), context)).value());
    }

    private boolean hasMember(RedisArray arr, String val) {
        for (RedisMessage m : arr.elements()) {
            if (((BulkString) m).asString().equals(val)) return true;
        }
        return false;
    }
}
