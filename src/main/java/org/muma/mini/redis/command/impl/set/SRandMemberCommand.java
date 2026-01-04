package org.muma.mini.redis.command.impl.set;

import org.muma.mini.redis.command.RedisCommand;
import org.muma.mini.redis.common.RedisData;
import org.muma.mini.redis.common.RedisDataType;
import org.muma.mini.redis.common.RedisSet;
import org.muma.mini.redis.protocol.*;
import org.muma.mini.redis.server.RedisContext;
import org.muma.mini.redis.store.StorageEngine;

import java.util.List;

/**
 * SRANDMEMBER key [count]
 */
public class SRandMemberCommand implements RedisCommand {
    @Override
    public RedisMessage execute(StorageEngine storage, RedisArray args, RedisContext context) {
        if (args.elements().length < 2) return errorArgs("srandmember");

        String key = ((BulkString) args.elements()[1]).asString();
        int count = 1;
        if (args.elements().length > 2) {
            try {
                assert ((BulkString) args.elements()[2]).asString() != null;
                count = Integer.parseInt(((BulkString) args.elements()[2]).asString());
            } catch (NumberFormatException e) {
                return errorInt();
            }
        }

        RedisData<?> data = storage.get(key);
        if (data == null) {
            return args.elements().length > 2 ? new RedisArray(new RedisMessage[0]) : new BulkString((byte[]) null);
        }
        if (data.getType() != RedisDataType.SET)
            return new ErrorMessage("WRONGTYPE Operation against a key holding the wrong kind of value");

        RedisSet set = data.getValue(RedisSet.class);

        // 这里的 randomMembers 实现需要在 Provider 中补全 (支持 count)
        // 我们在 IntSetProvider 和 HashTableSetProvider 已经预留了 randomMembers(count)
        List<byte[]> members = set.randomMembers(count);

        if (args.elements().length > 2) {
            RedisMessage[] result = new RedisMessage[members.size()];
            for (int i = 0; i < members.size(); i++) {
                result[i] = new BulkString(members.get(i));
            }
            return new RedisArray(result);
        } else {
            return members.isEmpty() ? new BulkString((byte[]) null) : new BulkString(members.get(0));
        }
    }
}
