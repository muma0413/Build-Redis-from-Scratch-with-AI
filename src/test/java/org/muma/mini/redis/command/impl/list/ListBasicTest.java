package org.muma.mini.redis.command.impl.list;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.muma.mini.redis.protocol.*;
import org.muma.mini.redis.server.RedisContext;
import org.muma.mini.redis.store.StorageEngine;
import org.muma.mini.redis.store.impl.MemoryStorageEngine;

import static org.junit.jupiter.api.Assertions.*;

class ListBasicTest {

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
    void testPushAndPop() {
        LPushCommand lpush = new LPushCommand();
        RPopCommand rpop = new RPopCommand();
        LLenCommand llen = new LLenCommand();

        // LPUSH k 1 2 3 -> [3, 2, 1]
        lpush.execute(storage, args("LPUSH", "k", "1", "2", "3"), context);
        assertEquals(3L, ((RedisInteger) llen.execute(storage, args("LLEN", "k"), context)).value());

        // RPOP -> 1
        RedisMessage res = rpop.execute(storage, args("RPOP", "k"), context);
        assertEquals("1", ((BulkString) res).asString());

        // RPOP -> 2
        res = rpop.execute(storage, args("RPOP", "k"), context);
        assertEquals("2", ((BulkString) res).asString());

        // RPOP -> 3 -> Empty -> Key Deleted
        res = rpop.execute(storage, args("RPOP", "k"), context);
        assertEquals("3", ((BulkString) res).asString());
        assertNull(storage.get("k"));
    }

    @Test
    void testIndexAndSet() {
        RPushCommand rpush = new RPushCommand();
        LIndexCommand lindex = new LIndexCommand();
        LSetCommand lset = new LSetCommand();

        rpush.execute(storage, args("RPUSH", "k", "a", "b", "c"), context);

        // LINDEX 1 -> b
        assertEquals("b", ((BulkString) lindex.execute(storage, args("LINDEX", "k", "1"), context)).asString());
        // LINDEX -1 -> c
        assertEquals("c", ((BulkString) lindex.execute(storage, args("LINDEX", "k", "-1"), context)).asString());

        // LSET 1 z -> [a, z, c]
        RedisMessage setRes = lset.execute(storage, args("LSET", "k", "1", "z"), context);
        assertTrue(setRes instanceof SimpleString);
        assertEquals("OK", ((SimpleString) setRes).content());

        assertEquals("z", ((BulkString) lindex.execute(storage, args("LINDEX", "k", "1"), context)).asString());
    }

    @Test
    void testPushX() {
        LPushXCommand lpushx = new LPushXCommand();
        RPushXCommand rpushx = new RPushXCommand();
        LPushCommand lpush = new LPushCommand();

        // 1. Key 不存在 -> 不做任何事，返回 0
        assertEquals(0L, ((RedisInteger) lpushx.execute(storage, args("LPUSHX", "kx", "v"), context)).value());
        assertEquals(0L, ((RedisInteger) rpushx.execute(storage, args("RPUSHX", "kx", "v"), context)).value());
        assertNull(storage.get("kx"));

        // 2. Key 存在 -> 正常 Push
        lpush.execute(storage, args("LPUSH", "kx", "head"), context); // [head]

        assertEquals(2L, ((RedisInteger) lpushx.execute(storage, args("LPUSHX", "kx", "new_head"), context)).value()); // [new_head, head]
        assertEquals(3L, ((RedisInteger) rpushx.execute(storage, args("RPUSHX", "kx", "tail"), context)).value()); // [new_head, head, tail]

        // 验证顺序
        LRangeCommand lrange = new LRangeCommand();
        RedisArray res = (RedisArray) lrange.execute(storage, args("LRANGE", "kx", "0", "-1"), context);
        assertEquals("new_head", ((BulkString)res.elements()[0]).asString());
        assertEquals("tail", ((BulkString)res.elements()[2]).asString());
    }
}
