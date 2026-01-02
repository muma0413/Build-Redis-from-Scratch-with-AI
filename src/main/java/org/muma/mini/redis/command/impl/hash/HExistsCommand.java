package org.muma.mini.redis.command.impl.hash;

import org.muma.mini.redis.command.RedisCommand;
import org.muma.mini.redis.common.RedisData;
import org.muma.mini.redis.common.RedisDataType;
import org.muma.mini.redis.common.RedisHash;
import org.muma.mini.redis.protocol.*;
import org.muma.mini.redis.store.StorageEngine;

/**
 * HEXISTS key field
 * 时间复杂度: O(1) for HashTable, O(N) for ZipList
 */
public class HExistsCommand implements RedisCommand {
    @Override
    public RedisMessage execute(StorageEngine storage, RedisArray args) {
        if (args.elements().length != 3) return new ErrorMessage("ERR wrong number of arguments for 'hexists'");

        String key = ((BulkString) args.elements()[1]).asString();
        String field = ((BulkString) args.elements()[2]).asString();

        RedisData<?> data = storage.get(key);
        if (data == null) return new RedisInteger(0);
        if (data.getType() != RedisDataType.HASH)
            return new ErrorMessage("WRONGTYPE Operation against a key holding the wrong kind of value");

        RedisHash hash = data.getValue(RedisHash.class);

        // get 返回 null 表示不存在 (RedisHash 内部实现约定)
        return hash.get(field) != null ? new RedisInteger(1) : new RedisInteger(0);
    }
}
