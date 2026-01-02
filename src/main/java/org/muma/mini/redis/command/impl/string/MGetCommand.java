package org.muma.mini.redis.command.impl.string;

import org.muma.mini.redis.command.RedisCommand;
import org.muma.mini.redis.common.RedisData;
import org.muma.mini.redis.common.RedisDataType;
import org.muma.mini.redis.protocol.*;
import org.muma.mini.redis.store.StorageEngine;

public class MGetCommand implements RedisCommand {
    @Override
    public RedisMessage execute(StorageEngine storage, RedisArray args) {
        // 格式: MGET key [key ...]
        RedisMessage[] elements = args.elements();
        if (elements.length < 2) {
            return new ErrorMessage("ERR wrong number of arguments for 'mget' command");
        }

        RedisMessage[] results = new RedisMessage[elements.length - 1];

        for (int i = 1; i < elements.length; i++) {
            String key = ((BulkString) elements[i]).asString();

            // 1. 使用通配符接收，避免 Raw Type 警告
            RedisData<?> data = storage.get(key);

            if (data == null || data.getType() != RedisDataType.STRING) {
                // Key 不存在或类型不对，Redis MGET 统一返回 nil
                results[i - 1] = new BulkString((byte[]) null);
            } else {
                // 2. 【修正核心】使用 getValue 安全获取泛型数据
                byte[] bytes = data.getValue(byte[].class);
                results[i - 1] = new BulkString(bytes);
            }
        }

        return new RedisArray(results);
    }
}
