package org.muma.mini.redis.command.impl.hash;

import org.muma.mini.redis.command.RedisCommand;
import org.muma.mini.redis.common.RedisData;
import org.muma.mini.redis.common.RedisDataType;
import org.muma.mini.redis.common.RedisHash;
import org.muma.mini.redis.protocol.*;
import org.muma.mini.redis.server.RedisContext;
import org.muma.mini.redis.store.StorageEngine;

import java.nio.charset.StandardCharsets;

/**
 * HINCRBY key field increment
 * 时间复杂度: O(1) for HashTable, O(N) for ZipList
 */
public class HIncrByCommand implements RedisCommand {
    @Override
    public RedisMessage execute(StorageEngine storage, RedisArray args, RedisContext context) {
        if (args.elements().length != 4) return new ErrorMessage("ERR wrong number of arguments for 'hincrby'");

        String key = ((BulkString) args.elements()[1]).asString();
        String field = ((BulkString) args.elements()[2]).asString();
        long increment;
        try {
            assert ((BulkString) args.elements()[3]).asString() != null;
            increment = Long.parseLong(((BulkString) args.elements()[3]).asString());
        } catch (NumberFormatException e) {
            return new ErrorMessage("ERR value is not an integer or out of range");
        }

        synchronized (storage.getLock(key)) {
            RedisData<?> data = storage.get(key);
            RedisHash hash;

            if (data == null) {
                hash = new RedisHash();
                data = new RedisData<>(RedisDataType.HASH, hash);
            } else {
                if (data.getType() != RedisDataType.HASH)
                    return new ErrorMessage("WRONGTYPE Operation against a key holding the wrong kind of value");
                hash = data.getValue(RedisHash.class);
            }

            // 1. 获取旧值
            byte[] oldBytes = hash.get(field);
            long oldVal = 0;
            if (oldBytes != null) {
                try {
                    oldVal = Long.parseLong(new String(oldBytes, StandardCharsets.UTF_8));
                } catch (NumberFormatException e) {
                    return new ErrorMessage("ERR hash value is not an integer");
                }
            }

            // 2. 计算新值
            long newVal = oldVal + increment;

            // 3. 存回 (会自动触发 ZipList -> HashTable 升级检查，虽然 long 很难超过 64字节)
            hash.put(field, String.valueOf(newVal).getBytes(StandardCharsets.UTF_8));

            storage.put(key, data); // 闭环回写

            return new RedisInteger(newVal);
        }
    }
}
