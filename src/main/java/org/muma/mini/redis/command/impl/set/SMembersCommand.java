package org.muma.mini.redis.command.impl.set;

import org.muma.mini.redis.command.RedisCommand;
import org.muma.mini.redis.common.RedisData;
import org.muma.mini.redis.common.RedisDataType;
import org.muma.mini.redis.common.RedisSet;
import org.muma.mini.redis.protocol.*;
import org.muma.mini.redis.server.RedisContext;
import org.muma.mini.redis.store.StorageEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * SMEMBERS key
 * Time Complexity: O(N)
 */
public class SMembersCommand implements RedisCommand {

    private static final Logger log = LoggerFactory.getLogger(SMembersCommand.class);

    @Override
    public RedisMessage execute(StorageEngine storage, RedisArray args, RedisContext context) {
        if (args.elements().length != 2) return errorArgs("smembers");

        String key = ((BulkString) args.elements()[1]).asString();
        RedisData<?> data = storage.get(key);

        if (data == null) return new RedisArray(new RedisMessage[0]);
        if (data.getType() != RedisDataType.SET) return new ErrorMessage("WRONGTYPE Operation against a key holding the wrong kind of value");

        RedisSet set = data.getValue(RedisSet.class);

        // 防御性日志
        if (set.size() > 10000) {
            log.warn("SMEMBERS called on large set '{}' with {} items.", key, set.size());
        }

        List<byte[]> members = set.getAll();
        RedisMessage[] result = new RedisMessage[members.size()];
        for (int i = 0; i < members.size(); i++) {
            result[i] = new BulkString(members.get(i));
        }
        return new RedisArray(result);
    }
}
