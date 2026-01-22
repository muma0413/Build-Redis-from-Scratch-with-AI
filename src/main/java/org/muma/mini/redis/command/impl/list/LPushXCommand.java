package org.muma.mini.redis.command.impl.list;

import org.muma.mini.redis.command.RedisCommand;
import org.muma.mini.redis.common.RedisData;
import org.muma.mini.redis.common.RedisDataType;
import org.muma.mini.redis.common.RedisList;
import org.muma.mini.redis.protocol.*;
import org.muma.mini.redis.server.RedisContext;
import org.muma.mini.redis.store.StorageEngine;

/**
 * LPUSHX key element [element ...]
 * <p>
 * 【时间复杂度】 O(K)
 * 仅当 key 存在时才推入。
 */
public class LPushXCommand implements RedisCommand {
    @Override
    public RedisMessage execute(StorageEngine storage, RedisArray args, RedisContext context) {
        RedisMessage[] elements = args.elements();
        if (elements.length < 3) return errorArgs("lpushx");

        String key = ((BulkString) elements[1]).asString();

        RedisData<?> data = storage.get(key);

        // 核心差异：如果 Key 不存在，什么都不做，直接返回 0
        if (data == null) return new RedisInteger(0);

        if (data.getType() != RedisDataType.LIST) {
            return new ErrorMessage("WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        RedisList list = data.getValue(RedisList.class);

        for (int i = 2; i < elements.length; i++) {
            byte[] val = ((BulkString) elements[i]).content();
            list.lpush(val);
        }

        storage.put(key, data);

        // 【Fix】先记录长度，再触发唤醒
        // 因为 onPush 可能会把数据弹走，导致 list.size() 变小
        long currentSize = list.size();

        // 4. 触发阻塞唤醒
        storage.getBlockingManager().onPush(key, storage);

        // 5. 返回推入后的长度 (快照)
        return new RedisInteger(currentSize);
    }

    @Override
    public boolean isWrite() {
        return true;
    }
}
