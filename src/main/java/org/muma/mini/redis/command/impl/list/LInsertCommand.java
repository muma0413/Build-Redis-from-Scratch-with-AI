package org.muma.mini.redis.command.impl.list;

import org.muma.mini.redis.command.RedisCommand;
import org.muma.mini.redis.common.RedisData;
import org.muma.mini.redis.common.RedisDataType;
import org.muma.mini.redis.common.RedisList;
import org.muma.mini.redis.protocol.*;
import org.muma.mini.redis.server.RedisContext;
import org.muma.mini.redis.store.StorageEngine;

import java.util.Locale;

/**
 * LINSERT key BEFORE|AFTER pivot element
 * <p>
 * 【时间复杂度】 O(N)
 * N 为寻找 pivot 过程中经过的元素数量。
 * <p>
 * 【功能】
 * 将值 element 插入到列表 key 当中，位于值 pivot 之前或之后。
 * 当 pivot 不存在于列表 key 时，不执行任何操作。
 * 当 key 不存在时，不执行任何操作。
 */
public class LInsertCommand implements RedisCommand {
    @Override
    public RedisMessage execute(StorageEngine storage, RedisArray args, RedisContext context) {
        if (args.elements().length != 5) return errorArgs("linsert");

        String key = ((BulkString) args.elements()[1]).asString();
        String where = ((BulkString) args.elements()[2]).asString().toUpperCase(Locale.ROOT);
        byte[] pivot = ((BulkString) args.elements()[3]).content();
        byte[] element = ((BulkString) args.elements()[4]).content();

        boolean before;
        if ("BEFORE".equals(where)) {
            before = true;
        } else if ("AFTER".equals(where)) {
            before = false;
        } else {
            return new ErrorMessage("ERR syntax error");
        }

        synchronized (storage.getLock(key)) {
            RedisData<?> data = storage.get(key);

            // Key 不存在，返回 0 (Redis 规范)
            if (data == null) return new RedisInteger(0);

            // 类型错误
            if (data.getType() != RedisDataType.LIST) {
                return new ErrorMessage("WRONGTYPE Operation against a key holding the wrong kind of value");
            }

            RedisList list = data.getValue(RedisList.class);

            // 核心操作: 调用 QuickList 的 insert
            // ret: -1 (pivot not found), >0 (new length)
            int ret = list.insert(before, pivot, element);

            // 只有当插入成功 (ret > 0) 时，才需要回写 Storage
            if (ret > 0) {
                storage.put(key, data);
            }

            return new RedisInteger(ret);
        }
    }
}
