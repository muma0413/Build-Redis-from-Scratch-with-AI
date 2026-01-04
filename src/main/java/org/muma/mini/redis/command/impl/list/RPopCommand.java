package org.muma.mini.redis.command.impl.list;

import org.muma.mini.redis.command.RedisCommand;
import org.muma.mini.redis.common.RedisData;
import org.muma.mini.redis.common.RedisDataType;
import org.muma.mini.redis.common.RedisList;
import org.muma.mini.redis.protocol.*;
import org.muma.mini.redis.server.RedisContext;
import org.muma.mini.redis.store.StorageEngine;

import java.util.ArrayList;
import java.util.List;

/**
 * RPOP key
 * <p>
 * 【时间复杂度】 O(1)
 */
public class RPopCommand implements RedisCommand {
    @Override
    public RedisMessage execute(StorageEngine storage, RedisArray args, RedisContext context) {
        if (args.elements().length < 2) return errorArgs("rpop");

        String key = ((BulkString) args.elements()[1]).asString();

        int count = 1;
        boolean hasCount = false;
        if (args.elements().length > 2) {
            try {
                assert ((BulkString) args.elements()[2]).asString() != null;
                count = Integer.parseInt(((BulkString) args.elements()[2]).asString());
                if (count < 0) return errorInt();
                hasCount = true;
            } catch (NumberFormatException e) {
                return errorInt();
            }
        }

        synchronized (storage.getLock(key)) {
            RedisData<?> data = storage.get(key);

            if (data == null) {
                return hasCount ? new RedisArray(new RedisMessage[0]) : new BulkString((byte[]) null);
            }
            if (data.getType() != RedisDataType.LIST) {
                return new ErrorMessage("WRONGTYPE Operation against a key holding the wrong kind of value");
            }

            RedisList list = data.getValue(RedisList.class);

            if (hasCount) {
                List<RedisMessage> result = new ArrayList<>();
                for (int i = 0; i < count; i++) {
                    byte[] val = list.rpop();
                    if (val == null) break;
                    result.add(new BulkString(val));
                }

                if (list.size() == 0) storage.remove(key);
                else storage.put(key, data);

                return new RedisArray(result.toArray(new RedisMessage[0]));
            } else {
                byte[] val = list.rpop();

                if (list.size() == 0) storage.remove(key);
                else storage.put(key, data);

                return val == null ? new BulkString((byte[]) null) : new BulkString(val);
            }
        }
    }
}
