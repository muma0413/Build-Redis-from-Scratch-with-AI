package org.muma.mini.redis.aof;

import org.muma.mini.redis.common.*;
import org.muma.mini.redis.config.MiniRedisConfig;
import org.muma.mini.redis.protocol.BulkString;
import org.muma.mini.redis.protocol.RedisArray;
import org.muma.mini.redis.protocol.RedisMessage;
import org.muma.mini.redis.store.StorageEngine;
import org.muma.mini.redis.utils.RespCodecUtil; // 之前写的工具类
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;


//[Main Thread]                               [Rewriter Thread]
//        |
//        (Append Check Trigger)
//        |
//        [Start Rewrite] -------------------------> (1. Start)
//        |                                         |
//        | <--- Atomic Switch Incr File ---        |
//        | Close Incr(N), Open Incr(N+1)           |
//        | Update Manifest: [Base, Incr(N), Incr(N+1)]
//        |                                         |
//        | (Continue Serving Clients)              | (2. Snapshot & Write)
//        | Write to Incr(N+1)                      | Iterate Memory -> NewBase.aof
//        |                                         |
//        |                                         | (3. Finish)
//        | <--- Callback / Notify Main ------------| Done!
//        |
//        [Finish Rewrite]
//        |
//        | Update Manifest: [NewBase, Incr(N+1)]
//        | Delete Old Files: Base(Old), Incr(N)
//        |
//END

/**
 * AOF 重写引擎
 * 负责遍历 StorageEngine，将内存数据转换为 RESP 写命令，写入新的 Base AOF 文件。
 */
public class AofRewriter {

    private static final Logger log = LoggerFactory.getLogger(AofRewriter.class);

    private final StorageEngine storage;

    public AofRewriter(StorageEngine storage) {
        this.storage = storage;
    }

    /**
     * 执行重写
     *
     * @param newBaseFile 目标文件 (通常是 temp 文件，写完后 rename)
     */
    public void rewrite(File newBaseFile) throws IOException {
        long start = System.currentTimeMillis();
        long count = 0;

        // 使用 BufferedOutputStream 提高写入性能 (减少磁盘 IO 次数)
        try (BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(newBaseFile))) {

            // 遍历所有 Key
            // 注意：StorageEngine 必须提供 keys() 方法，且迭代器最好是弱一致性的 (ConcurrentHashMap)
            for (String key : storage.keys()) {
                RedisData<?> data = storage.get(key);

                // 1. 过滤已过期或空数据
                if (data == null || data.isExpired()) continue;

                // 2. 转换为重建命令
                RedisArray cmd = objectToCommand(key, data);
                if (cmd != null) {
                    // 3. 编码为 RESP 字节流
                    byte[] bytes = RespCodecUtil.encode(cmd);
                    bos.write(bytes);
                    count++;

                    // 4. 处理过期时间 (PEXPIREAT)
                    // 如果 Key 有过期时间，需要追加一条 PEXPIREAT 命令
                    if (data.getExpireAt() != -1) {
                        RedisArray expireCmd = buildExpireCmd(key, data.getExpireAt());
                        bos.write(RespCodecUtil.encode(expireCmd));
                    }
                }
            }

            bos.flush();
            // 在关闭前强制刷盘，确保数据落盘
            // ((FileOutputStream) bos).getFD().sync(); // 可选，更加安全
        }

        long duration = System.currentTimeMillis() - start;
        log.info("AOF rewrite finished. Keys: {}, Size: {}, Duration: {} ms",
                count, newBaseFile.length(), duration);
    }

    // --- 核心转换逻辑 ---

    private RedisArray objectToCommand(String key, RedisData<?> data) {
        RedisDataType type = data.getType();

        switch (type) {
            case STRING:
                return buildStringCmd(key, (byte[]) data.getData());
            case LIST:
                return buildListCmd(key, (RedisList) data.getData());
            case HASH:
                return buildHashCmd(key, (RedisHash) data.getData());
            case SET:
                return buildSetCmd(key, (RedisSet) data.getData());
            case ZSET:
                return buildZSetCmd(key, (RedisZSet) data.getData());
            default:
                log.warn("Unknown data type during rewrite: {}", type);
                return null;
        }
    }

    // --- 各类型构建器 ---

    private RedisArray buildStringCmd(String key, byte[] val) {
        // SET key val
        return array("SET", key, new String(val, StandardCharsets.UTF_8));
    }

    private RedisArray buildListCmd(String key, RedisList list) {
        // RPUSH key v1 v2 ...
        List<byte[]> items = list.range(0, -1);
        if (items.isEmpty()) return null;

        // Redis 优化：如果 List 很大，会拆分成多条 RPUSH (每条 64 个参数)
        // Mini-Redis 简化：一条 RPUSH 搞定 (注意 RESP 协议没有限制，但缓冲区可能受限)
        // 为了稳健，我们这里也应该考虑分批，但为了代码简单，先全量。

        RedisMessage[] msgs = new RedisMessage[2 + items.size()];
        msgs[0] = new BulkString("RPUSH");
        msgs[1] = new BulkString(key);
        for (int i = 0; i < items.size(); i++) {
            msgs[2 + i] = new BulkString(items.get(i));
        }
        return new RedisArray(msgs);
    }

    private RedisArray buildHashCmd(String key, RedisHash hash) {
        // HMSET key f1 v1 f2 v2 ...
        // 注意：HMSET 在 Redis 4.0 后废弃，改用 HSET (支持多参)
        Map<String, byte[]> map = hash.toMap();
        if (map.isEmpty()) return null;

        RedisMessage[] msgs = new RedisMessage[2 + map.size() * 2];
        msgs[0] = new BulkString("HSET");
        msgs[1] = new BulkString(key);
        int i = 2;
        for (Map.Entry<String, byte[]> entry : map.entrySet()) {
            msgs[i++] = new BulkString(entry.getKey());
            msgs[i++] = new BulkString(entry.getValue());
        }
        return new RedisArray(msgs);
    }

    private RedisArray buildSetCmd(String key, RedisSet set) {
        // SADD key m1 m2 ...
        List<byte[]> members = set.getAll();
        if (members.isEmpty()) return null;

        RedisMessage[] msgs = new RedisMessage[2 + members.size()];
        msgs[0] = new BulkString("SADD");
        msgs[1] = new BulkString(key);
        for (int i = 0; i < members.size(); i++) {
            msgs[2 + i] = new BulkString(members.get(i));
        }
        return new RedisArray(msgs);
    }

    private RedisArray buildZSetCmd(String key, RedisZSet zset) {
        // ZADD key s1 m1 s2 m2 ...
        List<RedisZSet.ZSetEntry> entries = zset.range(0, -1);
        if (entries.isEmpty()) return null;

        RedisMessage[] msgs = new RedisMessage[2 + entries.size() * 2];
        msgs[0] = new BulkString("ZADD");
        msgs[1] = new BulkString(key);
        int i = 2;
        for (RedisZSet.ZSetEntry entry : entries) {
            // ZADD 格式: score member
            // 处理浮点数
            String scoreStr = (entry.score() % 1 == 0) ?
                    String.valueOf((long) entry.score()) : String.valueOf(entry.score());

            msgs[i++] = new BulkString(scoreStr);
            msgs[i++] = new BulkString(entry.member());
        }
        return new RedisArray(msgs);
    }

    private RedisArray buildExpireCmd(String key, long expireAt) {
        // PEXPIREAT key timestamp-ms
        return array("PEXPIREAT", key, String.valueOf(expireAt));
    }

    // --- 辅助 ---

    private RedisArray array(String... args) {
        RedisMessage[] msgs = new RedisMessage[args.length];
        for (int i = 0; i < args.length; i++) msgs[i] = new BulkString(args[i]);
        return new RedisArray(msgs);
    }
}
