package org.muma.mini.redis.command.impl.hash;

import org.muma.mini.redis.command.RedisCommand;
import org.muma.mini.redis.common.RedisData;
import org.muma.mini.redis.common.RedisDataType;
import org.muma.mini.redis.common.RedisHash;
import org.muma.mini.redis.protocol.*;
import org.muma.mini.redis.server.RedisContext;
import org.muma.mini.redis.store.StorageEngine;
import java.util.Map;

public class HGetAllCommand implements RedisCommand {
    @Override
    public RedisMessage execute(StorageEngine storage, RedisArray args, RedisContext context) {
        // HGETALL key
        if (args.elements().length != 2) {
            return new ErrorMessage("ERR wrong number of arguments for 'hgetall' command");
        }

        String key = ((BulkString) args.elements()[1]).asString();
        RedisData<?> redisData = storage.get(key);

        if (redisData == null) {
            return new RedisArray(new RedisMessage[0]); // 返回空数组
        }
        if (redisData.getType() != RedisDataType.HASH) {
            return new ErrorMessage("WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        RedisHash hash = redisData.getValue(RedisHash.class);

        // 获取内部 map (无论是 ZipList 还是 HashTable 实现的)
        // 注意：这里可能存在性能损耗，因为 toMap() 做了数据拷贝。
        // 在生产级实现中，应该让 HashProvider 返回一个 Iterator，以流式方式构建响应。
        Map<String, byte[]> all = hash.toMap();

        // 构造 RESP 数组: [key1, val1, key2, val2, ...]
        RedisMessage[] result = new RedisMessage[all.size() * 2];
        int i = 0;
        for (Map.Entry<String, byte[]> entry : all.entrySet()) {
            result[i++] = new BulkString(entry.getKey());
            result[i++] = new BulkString(entry.getValue());
        }

        return new RedisArray(result);
    }
}
