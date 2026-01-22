package org.muma.mini.redis.command.impl.set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.muma.mini.redis.common.RedisSet;
import org.muma.mini.redis.protocol.*;
import org.muma.mini.redis.server.RedisContext;
import org.muma.mini.redis.store.StorageEngine;
import org.muma.mini.redis.store.impl.MemoryStorageEngine;

import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class SetRandomTest {

    private StorageEngine storage;
    private RedisContext context;

    @BeforeEach
    void setUp() {
        storage = new MemoryStorageEngine();
        context = new RedisContext(null);
        new SAddCommand().execute(storage, args("SADD", "k", "1", "2", "3"), context);
    }

    private RedisArray args(String... args) {
        RedisMessage[] msgs = new RedisMessage[args.length];
        for (int i = 0; i < args.length; i++) msgs[i] = new BulkString(args[i]);
        return new RedisArray(msgs);
    }

    @Test
    void testSPop() {
        SPopCommand spop = new SPopCommand();

        // Pop 1
        RedisMessage res = spop.execute(storage, args("SPOP", "k"), context);
        assertTrue(res instanceof BulkString); // Single pop returns BulkString

        // Pop count
        RedisMessage resArr = spop.execute(storage, args("SPOP", "k", "2"), context);
        assertTrue(resArr instanceof RedisArray);
        assertEquals(2, ((RedisArray) resArr).elements().length);

        // Now empty
        assertNull(storage.get("k"));
    }

    @Test
    void testSRandMember() {
        SRandMemberCommand srand = new SRandMemberCommand();

        // Rand 1
        RedisMessage res = srand.execute(storage, args("SRANDMEMBER", "k"), context);
        String val = ((BulkString) res).asString();
        assertTrue(Set.of("1", "2", "3").contains(val));

        // Size should be same
        assertEquals(3, ((RedisSet)storage.get("k").getData()).size());
    }

    @Test
    void testSScan() {
        SScanCommand sscan = new SScanCommand();
        // SCAN k 0
        RedisArray res = (RedisArray) sscan.execute(storage, args("SSCAN", "k", "0"), context);

        RedisMessage[] els = res.elements();
        String cursor = ((BulkString) els[0]).asString();
        assertEquals("0", cursor); // Small set finishes in one go

        RedisArray items = (RedisArray) els[1];
        assertEquals(3, items.elements().length);
    }
}
