package org.muma.mini.redis.command.impl.bf;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.muma.mini.redis.common.RedisData;
import org.muma.mini.redis.common.RedisDataType;
import org.muma.mini.redis.protocol.*;
import org.muma.mini.redis.server.RedisContext;
import org.muma.mini.redis.store.StorageEngine;
import org.muma.mini.redis.store.impl.MemoryStorageEngine;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.*;

class BloomFilterTest {

    private StorageEngine storage;
    private RedisContext context;
    private BfAddCommand bfAdd;
    private BfExistsCommand bfExists;
    private BfReserveCommand bfReserve;

    @BeforeEach
    void setUp() {
        storage = new MemoryStorageEngine();
        context = new RedisContext(null);
        bfAdd = new BfAddCommand();
        bfExists = new BfExistsCommand();
        bfReserve = new BfReserveCommand();
    }

    private RedisArray args(String... args) {
        RedisMessage[] msgs = new RedisMessage[args.length];
        for (int i = 0; i < args.length; i++) msgs[i] = new BulkString(args[i]);
        return new RedisArray(msgs);
    }

    @Test
    void testBfAddAndExists() {
        // 1. 自动创建默认 BF
        // BF.ADD bf item1 -> 1 (New)
        RedisMessage res1 = bfAdd.execute(storage, args("BF.ADD", "bf", "item1"), context);
        assertEquals(1L, ((RedisInteger) res1).value());

        // 2. 检查存在性
        // BF.EXISTS bf item1 -> 1 (Exist)
        assertEquals(1L, ((RedisInteger) bfExists.execute(storage, args("BF.EXISTS", "bf", "item1"), context)).value());

        // BF.EXISTS bf item2 -> 0 (Not Exist)
        assertEquals(0L, ((RedisInteger) bfExists.execute(storage, args("BF.EXISTS", "bf", "item2"), context)).value());

        // 3. 重复添加
        // BF.ADD bf item1 -> 0 (Already Exists - BloomFilter 只能说可能存在，如果位都为1则返回0)
        RedisMessage res2 = bfAdd.execute(storage, args("BF.ADD", "bf", "item1"), context);
        assertEquals(0L, ((RedisInteger) res2).value());
    }

    @Test
    void testBfReserve() {
        // BF.RESERVE custom_bf 1000 5
        // Error rate is usually converted to m/k, here we use m k directly for simplicity
        RedisMessage res = bfReserve.execute(storage, args("BF.RESERVE", "custom_bf", "100", "3"), context);
        assertEquals("OK", ((SimpleString) res).content());

        // Verify underlying storage structure (Whitebox test)
        RedisData<?> data = storage.get("custom_bf");
        assertNotNull(data);
        assertEquals(RedisDataType.STRING, data.getType());

        byte[] bytes = data.getValue(byte[].class);
        // Header 8 bytes + Bitmap
        // m=100 -> bitmap len = (100+7)/8 = 13 bytes
        // Total = 8 + 13 = 21
        assertEquals(21, bytes.length);

        ByteBuffer buf = ByteBuffer.wrap(bytes);
        assertEquals(100, buf.getInt()); // m
        assertEquals(3, buf.getInt());   // k

        // Add item to custom bf
        bfAdd.execute(storage, args("BF.ADD", "custom_bf", "val"), context);
        assertEquals(1L, ((RedisInteger) bfExists.execute(storage, args("BF.EXISTS", "custom_bf", "val"), context)).value());
    }
}
