package org.muma.mini.redis.command.impl.set;

import org.muma.mini.redis.command.RedisCommand;
import org.muma.mini.redis.common.RedisData;
import org.muma.mini.redis.common.RedisDataType;
import org.muma.mini.redis.common.RedisSet;
import org.muma.mini.redis.protocol.*;
import org.muma.mini.redis.server.RedisContext;
import org.muma.mini.redis.store.StorageEngine;

/**
 * SPOP key [count]
 * Time Complexity: O(1) * count
 */
public class SPopCommand implements RedisCommand {
    @Override
    public RedisMessage execute(StorageEngine storage, RedisArray args, RedisContext context) {
        if (args.elements().length < 2) return errorArgs("spop");

        String key = ((BulkString) args.elements()[1]).asString();
        int count = 1;
        if (args.elements().length > 2) {
            try {
                count = Integer.parseInt(((BulkString) args.elements()[2]).asString());
            } catch (NumberFormatException e) {
                return errorInt();
            }
        }

        synchronized (storage.getLock(key)) {
            RedisData<?> data = storage.get(key);
            if (data == null) return new BulkString((byte[]) null); // 如果 count>1 应该返回空数组？Redis 3.2+ 是这样的
            if (data.getType() != RedisDataType.SET) return new ErrorMessage("WRONGTYPE Operation against a key holding the wrong kind of value");

            RedisSet set = data.getValue(RedisSet.class);

            // 如果指定了 count (即使是 1)，返回 Array
            // 如果没指定 count，返回 BulkString
            boolean isArrayResult = args.elements().length > 2;

            if (!isArrayResult) {
                byte[] val = set.pop();
                if (set.size() == 0) storage.remove(key);
                else storage.put(key, (RedisData<RedisSet>) data);
                return val == null ? new BulkString((byte[])null) : new BulkString(val);
            } else {
                int actualCount = Math.min(count, set.size());
                RedisMessage[] result = new RedisMessage[actualCount];
                for (int i = 0; i < actualCount; i++) {
                    byte[] val = set.pop();
                    result[i] = new BulkString(val);
                }

                if (set.size() == 0) storage.remove(key);
                else storage.put(key, (RedisData<RedisSet>) data);

                return new RedisArray(result);
            }
        }
    }
}
