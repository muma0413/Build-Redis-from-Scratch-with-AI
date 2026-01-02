package org.muma.mini.redis;


import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.muma.mini.redis.command.impl.hash.*;
import org.muma.mini.redis.common.RedisData;
import org.muma.mini.redis.common.RedisDataType;
import org.muma.mini.redis.protocol.*;
import org.muma.mini.redis.store.StorageEngine;
import org.muma.mini.redis.store.impl.MemoryStorageEngine;

import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class HashIntegrationTest {

    private StorageEngine storage;
    private HSetCommand hSet;
    private HGetCommand hGet;
    private HDelCommand hDel;
    private HGetAllCommand hGetAll;
    private HLenCommand hLen;
    private HExistsCommand hExists;
    private HIncrByCommand hIncrBy;
    private HKeysCommand hKeys;
    private HValsCommand hVals;

    @BeforeEach
    void setUp() {
        storage = new MemoryStorageEngine();
        hSet = new HSetCommand();
        hGet = new HGetCommand();
        hDel = new HDelCommand();
        hGetAll = new HGetAllCommand();
        hLen = new HLenCommand();
        hExists = new HExistsCommand();
        hIncrBy = new HIncrByCommand();
        hKeys = new HKeysCommand();
        hVals = new HValsCommand();
    }

    // --- 辅助方法：构建参数 ---
    private RedisArray args(String... args) {
        RedisMessage[] msgs = new RedisMessage[args.length];
        for (int i = 0; i < args.length; i++) {
            msgs[i] = new BulkString(args[i]);
        }
        return new RedisArray(msgs);
    }

    // --- 辅助方法：解析结果 ---
    private String asString(RedisMessage msg) {
        if (msg instanceof BulkString b) return b.asString();
        if (msg instanceof SimpleString s) return s.content();
        if (msg instanceof ErrorMessage e) return e.content();
        return null;
    }

    private long asLong(RedisMessage msg) {
        if (msg instanceof RedisInteger i) return i.value();
        throw new RuntimeException("Not an integer");
    }

    @Test
    void testHSetAndHGet() {
        // 1. 新增字段
        RedisMessage res1 = hSet.execute(storage, args("HSET", "user:1", "name", "root"));
        assertEquals(1, asLong(res1)); // 返回 1 (新字段)

        // 2. 更新字段
        RedisMessage res2 = hSet.execute(storage, args("HSET", "user:1", "name", "admin"));
        assertEquals(0, asLong(res2)); // 返回 0 (更新)

        // 3. 获取字段
        RedisMessage val = hGet.execute(storage, args("HGET", "user:1", "name"));
        assertEquals("admin", asString(val));

        // 4. 获取不存在的字段
        RedisMessage nilVal = hGet.execute(storage, args("HGET", "user:1", "age"));
        assertTrue(((BulkString) nilVal).content() == null); // Nil
    }

    @Test
    void testHDel() {
        hSet.execute(storage, args("HSET", "user:1", "f1", "v1"));
        hSet.execute(storage, args("HSET", "user:1", "f2", "v2"));

        // 1. 删除单个存在的字段
        RedisMessage res1 = hDel.execute(storage, args("HDEL", "user:1", "f1"));
        assertEquals(1, asLong(res1));

        // 2. 删除不存在的字段
        RedisMessage res2 = hDel.execute(storage, args("HDEL", "user:1", "xxx"));
        assertEquals(0, asLong(res2));

        // 3. 验证删除结果
        assertEquals(1, asLong(hLen.execute(storage, args("HLEN", "user:1"))));

        // 4. 删除最后一个字段，Key 应该消失 (Cleanup logic)
        hDel.execute(storage, args("HDEL", "user:1", "f2"));
        assertNull(storage.get("user:1"), "Key should be removed when hash is empty");
    }

    @Test
    void testHGetAll() {
        hSet.execute(storage, args("HSET", "u1", "k1", "v1"));
        hSet.execute(storage, args("HSET", "u1", "k2", "v2"));

        RedisArray result = (RedisArray) hGetAll.execute(storage, args("HGETALL", "u1"));
        RedisMessage[] elements = result.elements();

        assertEquals(4, elements.length);

        // 简单的包含测试 (顺序不一定保证，所以用 Set 验证)
        Set<String> resultSet = new HashSet<>();
        for (RedisMessage msg : elements) {
            resultSet.add(((BulkString) msg).asString());
        }
        assertTrue(resultSet.contains("k1"));
        assertTrue(resultSet.contains("v1"));
        assertTrue(resultSet.contains("k2"));
        assertTrue(resultSet.contains("v2"));
    }

    @Test
    void testHLenAndHExists() {
        hSet.execute(storage, args("HSET", "u1", "k1", "v1"));

        // HLEN
        assertEquals(1, asLong(hLen.execute(storage, args("HLEN", "u1"))));
        assertEquals(0, asLong(hLen.execute(storage, args("HLEN", "not_exist"))));

        // HEXISTS
        assertEquals(1, asLong(hExists.execute(storage, args("HEXISTS", "u1", "k1"))));
        assertEquals(0, asLong(hExists.execute(storage, args("HEXISTS", "u1", "k2"))));
    }

    @Test
    void testHIncrBy() {
        // 1. 对新 Key 自增
        RedisMessage res1 = hIncrBy.execute(storage, args("HINCRBY", "count", "page_view", "10"));
        assertEquals(10, asLong(res1));

        // 2. 再次自增
        RedisMessage res2 = hIncrBy.execute(storage, args("HINCRBY", "count", "page_view", "5"));
        assertEquals(15, asLong(res2));

        // 3. 减法 (负增量)
        RedisMessage res3 = hIncrBy.execute(storage, args("HINCRBY", "count", "page_view", "-20"));
        assertEquals(-5, asLong(res3));

        // 4. 错误类型测试: 设置非数字值
        hSet.execute(storage, args("HSET", "count", "title", "hello"));
        RedisMessage err = hIncrBy.execute(storage, args("HINCRBY", "count", "title", "1"));
        assertTrue(asString(err).startsWith("ERR hash value is not an integer"));
    }

    @Test
    void testHKeysAndHVals() {
        hSet.execute(storage, args("HSET", "u1", "k1", "v1"));
        hSet.execute(storage, args("HSET", "u1", "k2", "v2"));

        // HKEYS
        RedisArray keys = (RedisArray) hKeys.execute(storage, args("HKEYS", "u1"));
        assertEquals(2, keys.elements().length);
        // ... (验证内容略)

        // HVALS
        RedisArray vals = (RedisArray) hVals.execute(storage, args("HVALS", "u1"));
        assertEquals(2, vals.elements().length);
        // ... (验证内容略)
    }

    @Test
    void testWrongType() {
        // 先设一个 String
        storage.put("str_key", new RedisData<>(RedisDataType.STRING, "hello".getBytes()));

        // 尝试用 HSET 操作 String Key
        RedisMessage err = hSet.execute(storage, args("HSET", "str_key", "f", "v"));
        assertTrue(asString(err).startsWith("WRONGTYPE"));
    }
}
