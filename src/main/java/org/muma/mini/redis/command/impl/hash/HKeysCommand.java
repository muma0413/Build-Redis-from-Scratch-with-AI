package org.muma.mini.redis.command.impl.hash;

import org.muma.mini.redis.command.RedisCommand;
import org.muma.mini.redis.common.RedisData;
import org.muma.mini.redis.common.RedisDataType;
import org.muma.mini.redis.common.RedisHash;
import org.muma.mini.redis.protocol.*;
import org.muma.mini.redis.server.RedisContext;
import org.muma.mini.redis.store.StorageEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * HKEYS key
 * <p>
 * 【性能警告 / Performance Warning】
 * Time Complexity: O(N) where N is the size of the hash.
 * <p>
 * 风险提示:
 * 当 Hash 包含大量字段（Big Key）时，此命令可能会导致 Redis 主线程阻塞，
 * 进而导致服务对其他客户端不可用。
 * <p>
 * 建议方案:
 * 生产环境尽量避免对大 Key 使用 HKEYS。
 * 请使用 HSCAN 命令代替，以渐进式的方式迭代字段。
 */
public class HKeysCommand implements RedisCommand {

    private static final Logger log = LoggerFactory.getLogger(HKeysCommand.class);

    // 安全阈值：模拟生产环境的大 Key 保护配置
    // 超过这个数量，打印警告日志，或者在严格模式下可以直接拒绝服务
    private static final int WARNING_THRESHOLD = 10000;

    @Override
    public RedisMessage execute(StorageEngine storage, RedisArray args, RedisContext context) {
        if (args.elements().length != 2) return new ErrorMessage("ERR wrong number of arguments for 'hkeys'");

        String key = ((BulkString) args.elements()[1]).asString();
        RedisData<?> data = storage.get(key);

        if (data == null) return new RedisArray(new RedisMessage[0]);
        if (data.getType() != RedisDataType.HASH)
            return new ErrorMessage("WRONGTYPE Operation against a key holding the wrong kind of value");

        RedisHash hash = data.getValue(RedisHash.class);
        int size = hash.size();

        // --- [防御性代码] ---
        if (size > WARNING_THRESHOLD) {
            log.warn("Performance Alert: HKEYS executed on a BIG KEY '{}' with size {}. This may block the server. Suggest using HSCAN.", key, size);
            // 如果是严格模式，这里可以直接 return new ErrorMessage("ERR Hash is too large, use HSCAN");
            return new ErrorMessage("ERR Hash is too large, use HSCAN");
        }
        // ------------------

        Map<String, byte[]> map = hash.toMap();

        RedisMessage[] keys = new RedisMessage[size];
        int i = 0;
        for (String k : map.keySet()) {
            keys[i++] = new BulkString(k);
        }

        return new RedisArray(keys);
    }
}
