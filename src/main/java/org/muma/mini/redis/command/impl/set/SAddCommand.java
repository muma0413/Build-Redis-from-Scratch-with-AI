package org.muma.mini.redis.command.impl.set;

import org.muma.mini.redis.command.RedisCommand;
import org.muma.mini.redis.common.RedisData;
import org.muma.mini.redis.common.RedisDataType;
import org.muma.mini.redis.common.RedisSet;
import org.muma.mini.redis.protocol.*;
import org.muma.mini.redis.server.RedisContext;
import org.muma.mini.redis.store.StorageEngine;

public class SAddCommand implements RedisCommand {
    @Override
    public RedisMessage execute(StorageEngine storage, RedisArray args, RedisContext context) {
        RedisMessage[] elements = args.elements();
        if (elements.length < 3) return errorArgs("sadd");

        String key = ((BulkString) elements[1]).asString();
        int addedCount = 0;

        RedisData<?> data = storage.get(key);
        RedisSet set;

        if (data == null) {
            set = new RedisSet();
            data = new RedisData<>(RedisDataType.SET, set);
        } else {
            if (data.getType() != RedisDataType.SET) {
                return new ErrorMessage("WRONGTYPE Operation against a key holding the wrong kind of value");
            }
            set = data.getValue(RedisSet.class);
        }

        for (int i = 2; i < elements.length; i++) {
            byte[] member = ((BulkString) elements[i]).content();
            addedCount += set.add(member);
        }

        storage.put(key, data);

        return new RedisInteger(addedCount);
    }
}
