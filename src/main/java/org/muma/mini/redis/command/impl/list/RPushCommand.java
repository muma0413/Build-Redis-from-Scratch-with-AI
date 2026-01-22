package org.muma.mini.redis.command.impl.list;

import org.muma.mini.redis.command.RedisCommand;
import org.muma.mini.redis.common.RedisData;
import org.muma.mini.redis.common.RedisDataType;
import org.muma.mini.redis.common.RedisList;
import org.muma.mini.redis.protocol.*;
import org.muma.mini.redis.server.RedisContext;
import org.muma.mini.redis.store.StorageEngine;

/**
 * RPUSH key element [element ...]
 *
 * 功能：将一个或多个值插入到列表 key 的表尾(最右边)。
 * 如果 key 不存在，一个空列表会被创建并执行 RPUSH 操作。
 *
 * 【时间复杂度】 O(K)，K 是插入元素的数量。
 *
 * 【并发模型】
 * 本方法运行在 RedisCoreExecutor 单线程中，无需加锁。
 */
public class RPushCommand implements RedisCommand {

    @Override
    public RedisMessage execute(StorageEngine storage, RedisArray args, RedisContext context) {
        RedisMessage[] elements = args.elements();
        // 参数校验: RPUSH key val...
        if (elements.length < 3) return errorArgs("rpush");

        String key = ((BulkString) elements[1]).asString();

        // 1. 获取或创建 List
        RedisData<?> data = storage.get(key);
        RedisList list;

        if (data == null) {
            list = new RedisList();
            data = new RedisData<>(RedisDataType.LIST, list);
        } else {
            // 类型检查
            if (data.getType() != RedisDataType.LIST) {
                return new ErrorMessage("WRONGTYPE Operation against a key holding the wrong kind of value");
            }
            list = data.getValue(RedisList.class);
        }

        // 2. 依次推入元素
        for (int i = 2; i < elements.length; i++) {
            byte[] val = ((BulkString) elements[i]).content();
            list.rpush(val);
        }

        // 3. 更新存储 (触发 Dirty 计数和 AOF 钩子)
        storage.put(key, data);

        // 【Fix】先记录长度，再触发唤醒
        // 因为 onPush 可能会把数据弹走，导致 list.size() 变小
        long currentSize = list.size();

        // 4. 触发阻塞唤醒
        storage.getBlockingManager().onPush(key, storage);

        // 5. 返回推入后的长度 (快照)
        return new RedisInteger(currentSize);
    }

    @Override
    public boolean isWrite() {
        return true; // 标记为写命令，触发 AOF 追加和 Replication 传播
    }
}
