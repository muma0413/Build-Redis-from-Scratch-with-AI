package org.muma.mini.redis.command.impl.list;

import org.muma.mini.redis.command.RedisCommand;
import org.muma.mini.redis.common.RedisData;
import org.muma.mini.redis.common.RedisDataType;
import org.muma.mini.redis.common.RedisList;
import org.muma.mini.redis.protocol.*;
import org.muma.mini.redis.server.RedisContext;
import org.muma.mini.redis.store.StorageEngine;

/**
 * LREM key count element
 * <p>
 * 【时间复杂度】 O(N)
 * N 为列表的长度。
 * <p>
 * 【功能】
 * 根据参数 count 的值，移除列表中与参数 element 相等的元素。
 * - count > 0 : 从表头开始向表尾搜索，移除与 element 相等的元素，数量为 count。
 * - count < 0 : 从表尾开始向表头搜索，移除与 element 相等的元素，数量为 count 的绝对值。
 * - count = 0 : 移除表中所有与 element 相等的值。
 */
public class LRemCommand implements RedisCommand {
    @Override
    public RedisMessage execute(StorageEngine storage, RedisArray args, RedisContext context) {
        if (args.elements().length != 4) return errorArgs("lrem");

        String key = ((BulkString) args.elements()[1]).asString();
        long count;
        try {
            count = Long.parseLong(((BulkString) args.elements()[2]).asString());
        } catch (NumberFormatException e) {
            return errorInt();
        }
        byte[] element = ((BulkString) args.elements()[3]).content();

        // 1. 获取数据
        RedisData<?> data = storage.get(key);

        // Key 不存在，视为空列表，删除 0 个
        if (data == null) return new RedisInteger(0);

        // 类型检查
        if (data.getType() != RedisDataType.LIST) {
            return new ErrorMessage("WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        RedisList list = data.getValue(RedisList.class);

        // 2. 核心操作: 调用 QuickList 的 remove 逻辑
        int removedCount = list.remove(count, element);

        // 3. 后置处理: 如果 List 空了，删除 Key
        if (list.size() == 0) {
            storage.remove(key);
        } else {
            // 显式回写，保持语义闭环
            storage.put(key, data);
        }

        return new RedisInteger(removedCount);
    }
}
