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
 * LPOP key
 * <p>
 * 【时间复杂度】 O(1)
 */
public class LPopCommand implements RedisCommand {
    @Override
    public RedisMessage execute(StorageEngine storage, RedisArray args, RedisContext context) {
        if (args.elements().length < 2) return errorArgs("lpop");

        String key = ((BulkString) args.elements()[1]).asString();

        // 1. 解析可选 Count
        int count = 1;
        boolean hasCount = false;
        if (args.elements().length > 2) {
            try {
                assert ((BulkString) args.elements()[2]).asString() != null;
                count = Integer.parseInt(((BulkString) args.elements()[2]).asString());
                if (count < 0) return errorInt(); // Count 不能为负
                hasCount = true;
            } catch (NumberFormatException e) {
                return errorInt();
            }
        }

        RedisData<?> data = storage.get(key);

        // Key 不存在
        if (data == null) {
            // 如果指定了 count，返回空数组；否则返回 nil
            return hasCount ? new RedisArray(new RedisMessage[0]) : new BulkString((byte[]) null);
        }

        if (data.getType() != RedisDataType.LIST) {
            return new ErrorMessage("WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        RedisList list = data.getValue(RedisList.class);

        // 2. 执行弹出
        if (hasCount) {
            // 批量模式：返回 Array
            List<RedisMessage> result = new ArrayList<>();
            // 弹出 count 个，或者直到列表为空
            for (int i = 0; i < count; i++) {
                byte[] val = list.lpop();
                if (val == null) break; // List 已空
                result.add(new BulkString(val));
            }

            // 清理与回写
            if (list.size() == 0) storage.remove(key);
            else storage.put(key, data);

            return new RedisArray(result.toArray(new RedisMessage[0]));
        } else {
            // 单个模式：返回 BulkString
            byte[] val = list.lpop();

            if (list.size() == 0) storage.remove(key);
            else storage.put(key, data);

            return val == null ? new BulkString((byte[]) null) : new BulkString(val);
        }
    }
}
