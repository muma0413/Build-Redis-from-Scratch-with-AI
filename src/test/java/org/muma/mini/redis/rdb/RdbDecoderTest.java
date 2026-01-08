package org.muma.mini.redis.rdb;

import org.junit.jupiter.api.Test;
import org.muma.mini.redis.common.RedisHash;
import org.muma.mini.redis.common.RedisList;
import org.muma.mini.redis.common.RedisSet;
import org.muma.mini.redis.common.RedisZSet;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class RdbDecoderTest {

    @Test
    void testLoopbackString() throws IOException {
        // Encode
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        RdbEncoder encoder = new RdbEncoder(bos);
        encoder.writeString("hello world");

        // Decode
        ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
        RdbDecoder decoder = new RdbDecoder(bis);
        String restored = decoder.readStringUtf8();

        // Verify
        assertEquals("hello world", restored);
    }

    @Test
    void testLoopbackList() throws IOException {
        // Encode
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        RdbEncoder encoder = new RdbEncoder(bos);

        RedisList original = new RedisList();
        original.rpush("a".getBytes());
        original.rpush("b".getBytes());

        encoder.writeList(original);

        // Decode
        ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
        RdbDecoder decoder = new RdbDecoder(bis);

        RedisList restored = decoder.readList();

        // Verify
        assertEquals(2, restored.size());
        List<byte[]> items = restored.range(0, -1);
        assertArrayEquals("a".getBytes(), items.get(0));
        assertArrayEquals("b".getBytes(), items.get(1));
    }

    @Test
    void testLoopbackSet() throws IOException {
        // Encode
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        RdbEncoder encoder = new RdbEncoder(bos);

        RedisSet original = new RedisSet();
        original.add("x".getBytes());
        original.add("y".getBytes());

        encoder.writeSet(original);

        // Decode
        ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
        RdbDecoder decoder = new RdbDecoder(bis);

        RedisSet restored = decoder.readSet();

        // Verify
        assertEquals(2, restored.size());
        assertTrue(restored.contains("x".getBytes()));
        assertTrue(restored.contains("y".getBytes()));
    }

    @Test
    void testLoopbackHash() throws IOException {
        // Encode
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        RdbEncoder encoder = new RdbEncoder(bos);

        RedisHash original = new RedisHash();
        original.put("field1", "val1".getBytes());
        original.put("field2", "val2".getBytes());

        encoder.writeHash(original);

        // Decode
        ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
        RdbDecoder decoder = new RdbDecoder(bis);

        RedisHash restored = decoder.readHash();

        // Verify
        assertEquals(2, restored.size());
        assertArrayEquals("val1".getBytes(), restored.get("field1"));
        assertArrayEquals("val2".getBytes(), restored.get("field2"));
    }

    @Test
    void testLoopbackZSet() throws IOException {
        // Encode
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        RdbEncoder encoder = new RdbEncoder(bos);

        RedisZSet original = new RedisZSet();
        original.add(10.5, "player1");
        original.add(20.0, "player2");

        encoder.writeZSet(original);

        // Decode
        ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
        RdbDecoder decoder = new RdbDecoder(bis);

        RedisZSet restored = decoder.readZSet();

        // Verify
        assertEquals(2, restored.size());
        assertEquals(10.5, restored.getScore("player1"));
        assertEquals(20.0, restored.getScore("player2"));
        // 验证排名顺序
        assertEquals(0L, restored.getRank("player1"));
    }
}
