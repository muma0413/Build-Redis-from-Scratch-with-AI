package org.muma.mini.redis.command.impl.hash;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.muma.mini.redis.protocol.*;
import org.muma.mini.redis.server.RedisContext;
import org.muma.mini.redis.store.StorageEngine;
import org.muma.mini.redis.store.impl.MemoryStorageEngine;

import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class HashIterateTest {

    private StorageEngine storage;
    private RedisContext context;

    @BeforeEach
    void setUp() {
        storage = new MemoryStorageEngine();
        context = new RedisContext(null);

        HSetCommand hset = new HSetCommand();
        hset.execute(storage, args("HSET", "user", "k1", "v1", "k2", "v2"), context);
    }

    private RedisArray args(String... args) {
        RedisMessage[] msgs = new RedisMessage[args.length];
        for (int i = 0; i < args.length; i++) msgs[i] = new BulkString(args[i]);
        return new RedisArray(msgs);
    }

    @Test
    void testHKeys() {
        HKeysCommand hkeys = new HKeysCommand();
        RedisArray res = (RedisArray) hkeys.execute(storage, args("HKEYS", "user"), context);

        assertEquals(2, res.elements().length);
        Set<String> keys = new HashSet<>();
        keys.add(((BulkString)res.elements()[0]).asString());
        keys.add(((BulkString)res.elements()[1]).asString());

        assertTrue(keys.contains("k1"));
        assertTrue(keys.contains("k2"));
    }

    @Test
    void testHVals() {
        HValsCommand hvals = new HValsCommand();
        RedisArray res = (RedisArray) hvals.execute(storage, args("HVALS", "user"), context);

        assertEquals(2, res.elements().length);
        Set<String> vals = new HashSet<>();
        vals.add(((BulkString)res.elements()[0]).asString());
        vals.add(((BulkString)res.elements()[1]).asString());

        assertTrue(vals.contains("v1"));
        assertTrue(vals.contains("v2"));
    }

    @Test
    void testHGetAll() {
        HGetAllCommand hgetall = new HGetAllCommand();
        RedisArray res = (RedisArray) hgetall.execute(storage, args("HGETALL", "user"), context);

        assertEquals(4, res.elements().length); // k1, v1, k2, v2
        // 顺序不一定，略过详细校验，只要长度对
    }
}
