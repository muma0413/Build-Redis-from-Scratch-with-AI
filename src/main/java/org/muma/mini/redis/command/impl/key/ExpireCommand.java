package org.muma.mini.redis.command.impl.key;

import org.muma.mini.redis.command.RedisCommand;
import org.muma.mini.redis.common.RedisData;
import org.muma.mini.redis.protocol.*;
import org.muma.mini.redis.store.StorageEngine;

public class ExpireCommand implements RedisCommand {
    @Override
    public RedisMessage execute(StorageEngine storage, RedisArray args) {
        // 格式: EXPIRE key seconds
        if (args.elements().length != 3) {
            return new ErrorMessage("ERR wrong number of arguments for 'expire' command");
        }

        String key = ((BulkString) args.elements()[1]).asString();
        long seconds;
        try {
            assert ((BulkString) args.elements()[2]).asString() != null;
            seconds = Long.parseLong(((BulkString) args.elements()[2]).asString());
        } catch (NumberFormatException e) {
            return new ErrorMessage("ERR value is not an integer or out of range");
        }

        // 逻辑：取出数据 -> 设置时间 -> 放回存储(触发ttlMap更新)
        // 注意：这里需要锁保证原子性，防止在设置过期时数据被删除或修改
        synchronized (storage.getLock(key)) {
            RedisData data = storage.get(key);
            if (data == null) {
                return new RedisInteger(0); // Key 不存在
            }

            long expireAt = System.currentTimeMillis() + (seconds * 1000);
            data.setExpireAt(expireAt);

            // 重新 put 以触发 StorageEngine 内部 ttlMap 的更新逻辑
            storage.put(key, data);
        }

        return new RedisInteger(1); // 设置成功
    }
}
