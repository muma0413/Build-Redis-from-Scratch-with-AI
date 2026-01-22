package org.muma.mini.redis.command.impl.set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.muma.mini.redis.protocol.*;
import org.muma.mini.redis.server.RedisContext;
import org.muma.mini.redis.store.StorageEngine;
import org.muma.mini.redis.store.impl.MemoryStorageEngine;

import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class SetBasicTest {

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
    void testSAddAndSIsMember() {
        SAddCommand sadd = new SAddCommand();
        SIsMemberCommand sismember = new SIsMemberCommand();

        // SADD k 1 2 3
        assertEquals(3L, ((RedisInteger) sadd.execute(storage, args("SADD", "k", "1", "2", "3"), context)).value());
        // SADD k 1 (duplicate)
        assertEquals(0L, ((RedisInteger) sadd.execute(storage, args("SADD", "k", "1"), context)).value());

        // SISMEMBER
        assertEquals(1L, ((RedisInteger) sismember.execute(storage, args("SISMEMBER", "k", "2"), context)).value());
        assertEquals(0L, ((RedisInteger) sismember.execute(storage, args("SISMEMBER", "k", "99"), context)).value());
    }

    @Test
    void testSRemAndSCard() {
        SAddCommand sadd = new SAddCommand();
        SRemCommand srem = new SRemCommand();
        SCardCommand scard = new SCardCommand();

        sadd.execute(storage, args("SADD", "k", "a", "b", "c"), context);
        assertEquals(3L, ((RedisInteger) scard.execute(storage, args("SCARD", "k"), context)).value());

        // SREM k a
        assertEquals(1L, ((RedisInteger) srem.execute(storage, args("SREM", "k", "a"), context)).value());
        assertEquals(2L, ((RedisInteger) scard.execute(storage, args("SCARD", "k"), context)).value());

        // SREM non-exist
        assertEquals(0L, ((RedisInteger) srem.execute(storage, args("SREM", "k", "z"), context)).value());

        // Remove all -> key deleted
        srem.execute(storage, args("SREM", "k", "b"), context);
        srem.execute(storage, args("SREM", "k", "c"), context);
        assertNull(storage.get("k"));
    }

    @Test
    void testSMembers() {
        SAddCommand sadd = new SAddCommand();
        SMembersCommand smembers = new SMembersCommand();

        sadd.execute(storage, args("SADD", "k", "x", "y"), context);
        RedisArray res = (RedisArray) smembers.execute(storage, args("SMEMBERS", "k"), context);

        assertEquals(2, res.elements().length);
        Set<String> members = new HashSet<>();
        members.add(((BulkString) res.elements()[0]).asString());
        members.add(((BulkString) res.elements()[1]).asString());

        assertTrue(members.contains("x"));
        assertTrue(members.contains("y"));
    }
}
