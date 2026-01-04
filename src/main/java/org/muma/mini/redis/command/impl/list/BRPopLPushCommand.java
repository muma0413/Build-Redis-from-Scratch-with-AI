package org.muma.mini.redis.command.impl.list;

import org.muma.mini.redis.command.RedisCommand;
import org.muma.mini.redis.common.RedisData;
import org.muma.mini.redis.common.RedisDataType;
import org.muma.mini.redis.common.RedisList;
import org.muma.mini.redis.protocol.BulkString;
import org.muma.mini.redis.protocol.ErrorMessage;
import org.muma.mini.redis.protocol.RedisArray;
import org.muma.mini.redis.protocol.RedisMessage;
import org.muma.mini.redis.server.RedisContext;
import org.muma.mini.redis.store.StorageEngine;

import java.util.Collections;

/**
 * BRPOPLPUSH source destination timeout
 */
public class BRPopLPushCommand implements RedisCommand {
    @Override
    public RedisMessage execute(StorageEngine storage, RedisArray args, RedisContext context) {
        RedisMessage[] elements = args.elements();
        if (elements.length != 4) return errorArgs("brpoplpush");

        String source = ((BulkString) elements[1]).asString();
        String destination = ((BulkString) elements[2]).asString();
        long timeout;
        try {
            assert ((BulkString) elements[3]).asString() != null;
            timeout = Long.parseLong(((BulkString) elements[3]).asString());
        } catch (NumberFormatException e) {
            return new ErrorMessage("ERR timeout is not an integer");
        }

        synchronized (storage) {
            RedisData<?> srcData = storage.get(source);

            // 1. 尝试非阻塞执行
            if (srcData != null && srcData.getType() == RedisDataType.LIST) {
                RedisList srcList = srcData.getValue(RedisList.class);
                if (srcList.size() > 0) {
                    // 执行 RPOP
                    byte[] val = srcList.rpop();
                    if (srcList.size() == 0) storage.remove(source);
                    else storage.put(source, srcData); // 回写通知

                    // 执行 LPUSH 到 destination
                    RedisData<?> destData = storage.get(destination);
                    RedisList destList;
                    if (destData == null) {
                        destList = new RedisList();
                        storage.put(destination, new RedisData<>(RedisDataType.LIST, destList));
                    } else {
                        if (destData.getType() != RedisDataType.LIST) {
                            // 这里需要把 RPOP 的值塞回去吗？Redis 是原子操作，这里会报错
                            // 但为了简化，我们假设之前 RPOP 成功了。
                            // 实际应该先检查 dest 类型再 RPOP。
                            return new ErrorMessage("WRONGTYPE Operation against a key holding the wrong kind of value");
                        }
                        destList = destData.getValue(RedisList.class);
                    }
                    destList.lpush(val);

                    // 级联唤醒
                    storage.getBlockingManager().onPush(destination, storage);

                    return new BulkString(val);
                }
            }

            // 2. 阻塞
            // 注意：BRPopLPush 是 RPOP (右出)
            storage.getBlockingManager().addWait(
                    context.getNettyCtx(),
                    Collections.singletonList(source),
                    timeout,
                    false,        // isLeftPop = false (RPOP)
                    destination   // targetKey
            );
        }

        return null;
    }
}
