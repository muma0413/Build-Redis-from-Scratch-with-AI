package org.muma.mini.redis.command.impl.list;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.muma.mini.redis.protocol.*;
import org.muma.mini.redis.server.RedisContext;
import org.muma.mini.redis.store.StorageEngine;
import org.muma.mini.redis.store.impl.MemoryStorageEngine;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ListOpsTest {

    private StorageEngine storage;
    private RedisContext context;
    private RPushCommand rpush;

    @BeforeEach
    void setUp() {
        storage = new MemoryStorageEngine();
        context = new RedisContext(null);
        rpush = new RPushCommand();
        rpush.execute(storage, args("RPUSH", "k", "a", "b", "c", "d", "e"), context);
    }

    private RedisArray args(String... args) {
        RedisMessage[] msgs = new RedisMessage[args.length];
        for (int i = 0; i < args.length; i++) msgs[i] = new BulkString(args[i]);
        return new RedisArray(msgs);
    }

    @Test
    void testLRange() {
        LRangeCommand lrange = new LRangeCommand();
        // LRANGE k 0 -1
        RedisArray res = (RedisArray) lrange.execute(storage, args("LRANGE", "k", "0", "-1"), context);
        assertEquals(5, res.elements().length);
        assertEquals("a", ((BulkString)res.elements()[0]).asString());
        assertEquals("e", ((BulkString)res.elements()[4]).asString());

        // LRANGE k 1 2 -> b, c
        res = (RedisArray) lrange.execute(storage, args("LRANGE", "k", "1", "2"), context);
        assertEquals(2, res.elements().length);
        assertEquals("b", ((BulkString)res.elements()[0]).asString());
    }

    @Test
    void testLInsert() {
        LInsertCommand linsert = new LInsertCommand();
        // LINSERT k BEFORE c x -> [a, b, x, c, d, e]
        linsert.execute(storage, args("LINSERT", "k", "BEFORE", "c", "x"), context);

        LIndexCommand lindex = new LIndexCommand();
        assertEquals("x", ((BulkString)lindex.execute(storage, args("LINDEX", "k", "2"), context)).asString());
    }

    @Test
    void testLRem() {
        // Prepare: [a, a, b, a]
        storage.remove("k");
        rpush.execute(storage, args("RPUSH", "k", "a", "a", "b", "a"), context);

        LRemCommand lrem = new LRemCommand();
        // LREM k 2 a -> remove 2 'a' from head -> [b, a]
        RedisMessage res = lrem.execute(storage, args("LREM", "k", "2", "a"), context);
        assertEquals(2L, ((RedisInteger)res).value());

        LLenCommand llen = new LLenCommand();
        assertEquals(2L, ((RedisInteger)llen.execute(storage, args("LLEN", "k"), context)).value());
    }

    @Test
    void testLTrim() {
        LTrimCommand ltrim = new LTrimCommand();
        // [a, b, c, d, e]
        // LTRIM k 1 3 -> [b, c, d]
        ltrim.execute(storage, args("LTRIM", "k", "1", "3"), context);

        LLenCommand llen = new LLenCommand();
        assertEquals(3L, ((RedisInteger)llen.execute(storage, args("LLEN", "k"), context)).value());

        LIndexCommand lindex = new LIndexCommand();
        assertEquals("b", ((BulkString)lindex.execute(storage, args("LINDEX", "k", "0"), context)).asString());
    }
}
