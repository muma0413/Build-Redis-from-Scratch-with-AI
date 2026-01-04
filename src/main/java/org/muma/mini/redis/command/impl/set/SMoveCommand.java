package org.muma.mini.redis.command.impl.set;

import org.muma.mini.redis.command.RedisCommand;
import org.muma.mini.redis.common.RedisData;
import org.muma.mini.redis.common.RedisDataType;
import org.muma.mini.redis.common.RedisSet;
import org.muma.mini.redis.protocol.*;
import org.muma.mini.redis.server.RedisContext;
import org.muma.mini.redis.store.StorageEngine;

/**
 * SMOVE source destination member
 * Time Complexity: O(1)
 */
public class SMoveCommand implements RedisCommand {
    @Override
    public RedisMessage execute(StorageEngine storage, RedisArray args, RedisContext context) {
        if (args.elements().length != 4) return errorArgs("smove");

        String source = ((BulkString) args.elements()[1]).asString();
        String destination = ((BulkString) args.elements()[2]).asString();
        byte[] member = ((BulkString) args.elements()[3]).content();

        // 必须全库锁或者同时锁两个Key (按顺序锁避免死锁)
        // 简单起见，全库锁
        synchronized (storage) {
            RedisData<?> srcData = storage.get(source);
            if (srcData == null) return new RedisInteger(0);
            if (srcData.getType() != RedisDataType.SET) return new ErrorMessage("WRONGTYPE Operation against a key holding the wrong kind of value");

            RedisSet srcSet = srcData.getValue(RedisSet.class);
            if (!srcSet.contains(member)) {
                return new RedisInteger(0); // Member 不存在
            }

            // 获取/创建目标集合
            RedisData<?> destData = storage.get(destination);
            RedisSet destSet;
            if (destData == null) {
                destSet = new RedisSet();
                // 只有成功移动后才 put，但这里我们先拿到对象
            } else if (destData.getType() != RedisDataType.SET) {
                return new ErrorMessage("WRONGTYPE Operation against a key holding the wrong kind of value");
            } else {
                destSet = destData.getValue(RedisSet.class);
            }

            // 执行移动 (原子操作)
            srcSet.remove(member);
            destSet.add(member);

            // 维护 Storage 状态
            if (srcSet.size() == 0) storage.remove(source);
            else storage.put(source, srcData); // 回写通知

            if (destData == null) {
                storage.put(destination, new RedisData<>(RedisDataType.SET, destSet));
            } else {
                storage.put(destination, destData);
            }

            return new RedisInteger(1);
        }
    }
}
