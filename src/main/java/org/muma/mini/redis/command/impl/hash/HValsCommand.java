package org.muma.mini.redis.command.impl.hash;

import org.muma.mini.redis.command.RedisCommand;
import org.muma.mini.redis.common.RedisData;
import org.muma.mini.redis.common.RedisDataType;
import org.muma.mini.redis.common.RedisHash;
import org.muma.mini.redis.protocol.*;
import org.muma.mini.redis.store.StorageEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * HVALS key
 * <p>
 * 【性能警告 / Performance Warning】
 * Time Complexity: O(N) where N is the size of the hash.
 * <p>
 * 风险提示:
 * 此命令需要构建所有 Value 的副本，不仅消耗 CPU 遍历，还会瞬间消耗大量内存用于构建 Response。
 * 如果 Value 本身较大，极易引发 OOM (Out Of Memory)。
 * <p>
 * 建议方案:
 * 请使用 HSCAN 命令代替。
 */
public class HValsCommand implements RedisCommand {

    private static final Logger log = LoggerFactory.getLogger(HValsCommand.class);
    private static final int WARNING_THRESHOLD = 10000;

    @Override
    public RedisMessage execute(StorageEngine storage, RedisArray args) {
        if (args.elements().length != 2) return new ErrorMessage("ERR wrong number of arguments for 'hvals'");

        String key = ((BulkString) args.elements()[1]).asString();
        RedisData<?> data = storage.get(key);

        if (data == null) return new RedisArray(new RedisMessage[0]);
        if (data.getType() != RedisDataType.HASH)
            return new ErrorMessage("WRONGTYPE Operation against a key holding the wrong kind of value");

        RedisHash hash = data.getValue(RedisHash.class);
        int size = hash.size();

        // --- [防御性代码] ---
        if (size > WARNING_THRESHOLD) {
            log.warn("Performance Alert: HVALS executed on a BIG KEY '{}' with size {}. Watch out for memory spikes!", key, size);
            return new ErrorMessage("ERR Hash is too large, use HSCAN");
        }
        // ------------------

        Map<String, byte[]> map = hash.toMap();

        RedisMessage[] vals = new RedisMessage[size];
        int i = 0;
        for (byte[] v : map.values()) {
            vals[i++] = new BulkString(v);
        }

        return new RedisArray(vals);
    }
}
