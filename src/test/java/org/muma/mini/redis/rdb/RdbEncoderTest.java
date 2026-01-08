package org.muma.mini.redis.rdb;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.muma.mini.redis.common.RedisHash;
import org.muma.mini.redis.common.RedisList;
import org.muma.mini.redis.common.RedisSet;
import org.muma.mini.redis.common.RedisZSet;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

class RdbEncoderTest {

    private ByteArrayOutputStream bos;
    private RdbEncoder encoder;

    @BeforeEach
    void setUp() {
        bos = new ByteArrayOutputStream();
        encoder = new RdbEncoder(bos);
    }

    @Test
    void testWriteLength() throws IOException {
        // Case 1: < 64 (0x05)
        encoder.writeLength(5);
        assertArrayEquals(new byte[]{0x05}, bos.toByteArray());
        bos.reset();

        // Case 2: > 64 (100) -> 01xxxxxx (0x40 | 0) (100) -> 0x40 0x64
        encoder.writeLength(100);
        assertArrayEquals(new byte[]{0x40, 0x64}, bos.toByteArray());
        bos.reset();

        // Case 3: > 16384 (20000) -> 0x80 + int(20000)
        encoder.writeLength(20000);
        // 20000 = 0x00004E20 (Big Endian: 00 00 4E 20)
        assertArrayEquals(new byte[]{(byte) 0x80, 0, 0, 0x4E, 0x20}, bos.toByteArray());
    }

    @Test
    void testWriteString() throws IOException {
        encoder.writeString("foo");
        // len=3 (0x03), 'f', 'o', 'o'
        assertArrayEquals(new byte[]{0x03, 'f', 'o', 'o'}, bos.toByteArray());
    }

    @Test
    void testWriteList() throws IOException {
        RedisList list = new RedisList();
        list.rpush("a".getBytes(StandardCharsets.UTF_8));
        list.rpush("b".getBytes(StandardCharsets.UTF_8));

        encoder.writeList(list);

        // Expected:
        // [Len=2] (0x02)
        // [Len=1] (0x01) "a" ('a')
        // [Len=1] (0x01) "b" ('b')
        assertArrayEquals(new byte[]{0x02, 0x01, 'a', 0x01, 'b'}, bos.toByteArray());
    }

    @Test
    void testWriteSet() throws IOException {
        RedisSet set = new RedisSet();
        set.add("a".getBytes(StandardCharsets.UTF_8));

        encoder.writeSet(set);

        // Expected:
        // [Len=1] (0x01)
        // [Len=1] (0x01) "a" ('a')
        assertArrayEquals(new byte[]{0x01, 0x01, 'a'}, bos.toByteArray());
    }

    @Test
    void testWriteHash() throws IOException {
        RedisHash hash = new RedisHash();
        hash.put("k", "v".getBytes(StandardCharsets.UTF_8));

        encoder.writeHash(hash);

        // Expected:
        // [Len=1] (0x01)
        // [Len=1] (0x01) "k" ('k')
        // [Len=1] (0x01) "v" ('v')
        assertArrayEquals(new byte[]{0x01, 0x01, 'k', 0x01, 'v'}, bos.toByteArray());
    }

    @Test
    void testWriteZSet() throws IOException {
        RedisZSet zset = new RedisZSet();
        zset.add(10.0, "m");

        encoder.writeZSet(zset);

        // Expected:
        // [Len=1] (0x01)
        // [Len=1] (0x01) "m" ('m')
        // [Len=2] (0x02) "10" ('1', '0')  (Score converted to String)
        assertArrayEquals(new byte[]{0x01, 0x01, 'm', 0x02, '1', '0'}, bos.toByteArray());
    }
}
