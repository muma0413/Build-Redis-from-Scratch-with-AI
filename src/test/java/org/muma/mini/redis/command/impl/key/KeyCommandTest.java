package org.muma.mini.redis.command.impl.key;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.muma.mini.redis.command.impl.string.SetCommand;
import org.muma.mini.redis.protocol.*;
import org.muma.mini.redis.server.RedisContext;
import org.muma.mini.redis.store.StorageEngine;
import org.muma.mini.redis.store.impl.MemoryStorageEngine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KeyCommandTest {

    private StorageEngine storage;
    private RedisContext context;

    @BeforeEach
    void setUp() {
        storage = new MemoryStorageEngine();
        context = new RedisContext(null);
        new SetCommand().execute(storage, args("SET", "k1", "v1"), context);
        new SetCommand().execute(storage, args("SET", "k2", "v2"), context);
    }

    private RedisArray args(String... args) {
        RedisMessage[] msgs = new RedisMessage[args.length];
        for (int i = 0; i < args.length; i++) msgs[i] = new BulkString(args[i]);
        return new RedisArray(msgs);
    }

    @Test
    void testExists() {
        ExistsCommand exists = new ExistsCommand();
        assertEquals(2L, ((RedisInteger) exists.execute(storage, args("EXISTS", "k1", "k2"), context)).value());
        assertEquals(1L, ((RedisInteger) exists.execute(storage, args("EXISTS", "k1", "nx"), context)).value());
    }

    @Test
    void testDel() {
        DelCommand del = new DelCommand();
        // Del 2 keys
        assertEquals(2L, ((RedisInteger) del.execute(storage, args("DEL", "k1", "k2", "nx"), context)).value());
        // Verify
        assertEquals(0L, ((RedisInteger) new ExistsCommand().execute(storage, args("EXISTS", "k1"), context)).value());
    }

    @Test
    void testExpireAndTTL() {
        ExpireCommand expire = new ExpireCommand();
        TTLCommand ttl = new TTLCommand();

        // No expire
        assertEquals(-1L, ((RedisInteger) ttl.execute(storage, args("TTL", "k1"), context)).value());

        // Set expire 10s
        expire.execute(storage, args("EXPIRE", "k1", "10"), context);
        long t = ((RedisInteger) ttl.execute(storage, args("TTL", "k1"), context)).value();
        assertTrue(t > 0 && t <= 10);

        // NX option: fail because exists
        assertEquals(0L, ((RedisInteger) expire.execute(storage, args("EXPIRE", "k1", "20", "NX"), context)).value());
    }
}
