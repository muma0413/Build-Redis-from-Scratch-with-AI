package org.muma.mini.redis.command.impl.string;

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

class StringBitmapTest {

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
    void testSetBitAndGetBit() {
        SetBitCommand setbit = new SetBitCommand();
        GetBitCommand getbit = new GetBitCommand();

        // SETBIT k 7 1  -> 00000001 (0x01)
        RedisMessage res = setbit.execute(storage, args("SETBIT", "k", "7", "1"), context);
        assertEquals(0L, ((RedisInteger) res).value()); // old val

        // GETBIT k 7 -> 1
        assertEquals(1L, ((RedisInteger) getbit.execute(storage, args("GETBIT", "k", "7"), context)).value());
        // GETBIT k 0 -> 0
        assertEquals(0L, ((RedisInteger) getbit.execute(storage, args("GETBIT", "k", "0"), context)).value());
    }

    @Test
    void testBitCount() {
        SetBitCommand setbit = new SetBitCommand();
        BitCountCommand bitcount = new BitCountCommand();

        // 00000001 00000001 (Set 7 and 15)
        setbit.execute(storage, args("SETBIT", "k", "7", "1"), context);
        setbit.execute(storage, args("SETBIT", "k", "15", "1"), context);

        assertEquals(2L, ((RedisInteger) bitcount.execute(storage, args("BITCOUNT", "k"), context)).value());
    }

    @Test
    void testBitOp() {
        SetCommand set = new SetCommand();
        BitOpCommand bitop = new BitOpCommand();
        GetCommand get = new GetCommand();

        // k1: \x0F (00001111)
        // k2: \x01 (00000001)
        // AND -> 00000001
        // OR  -> 00001111
        // XOR -> 00001110

        // 构造数据有点麻烦，用 char
        byte[] b1 = new byte[]{0x0F};
        byte[] b2 = new byte[]{0x01};
        // Hack: 直接通过 set 写入 raw bytes (这里 args 是 String，实际使用需要 encode)
        // 简单起见，我们用 ASCII 字符
        // 'a' = 01100001 (97)
        // 'b' = 01100010 (98)
        // AND = 01100000 (96) = '`'
        set.execute(storage, args("SET", "k1", "a"), context);
        set.execute(storage, args("SET", "k2", "b"), context);

        bitop.execute(storage, args("BITOP", "AND", "dest", "k1", "k2"), context);

        RedisMessage res = get.execute(storage, args("GET", "dest"), context);
        assertEquals("`", ((BulkString) res).asString());
    }
}
