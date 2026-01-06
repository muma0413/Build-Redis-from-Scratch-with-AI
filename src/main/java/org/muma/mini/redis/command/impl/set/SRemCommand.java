package org.muma.mini.redis.command.impl.set;

import org.muma.mini.redis.command.RedisCommand;
import org.muma.mini.redis.common.RedisData;
import org.muma.mini.redis.common.RedisDataType;
import org.muma.mini.redis.common.RedisSet;
import org.muma.mini.redis.protocol.*;
import org.muma.mini.redis.server.RedisContext;
import org.muma.mini.redis.store.StorageEngine;

/**
 * SREM key member [member ...]
 * Time Complexity: O(N)
 */
public class SRemCommand implements RedisCommand {
    @Override
    public RedisMessage execute(StorageEngine storage, RedisArray args, RedisContext context) {
        if (args.elements().length < 3) return errorArgs("srem");

        String key = ((BulkString) args.elements()[1]).asString();
        RedisMessage[] elements = args.elements();
        int removedCount = 0;

        RedisData<?> data = storage.get(key);
        if (data == null) return new RedisInteger(0);
        if (data.getType() != RedisDataType.SET)
            return new ErrorMessage("WRONGTYPE Operation against a key holding the wrong kind of value");

        RedisSet set = data.getValue(RedisSet.class);

        for (int i = 2; i < elements.length; i++) {
            byte[] member = ((BulkString) elements[i]).content();
            removedCount += set.remove(member);
        }

        if (set.size() == 0) {
            storage.remove(key);
        } else {
            // 显式回写
            storage.put(key, data);
        }
        return new RedisInteger(removedCount);
    }

    @Override
    public boolean isWrite() {
        return true;
    }
}
