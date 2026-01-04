package org.muma.mini.redis.command.impl.set;

import org.muma.mini.redis.command.RedisCommand;
import org.muma.mini.redis.common.RedisData;
import org.muma.mini.redis.common.RedisDataType;
import org.muma.mini.redis.common.RedisSet;
import org.muma.mini.redis.protocol.*;
import org.muma.mini.redis.server.RedisContext;
import org.muma.mini.redis.store.StorageEngine;
import org.muma.mini.redis.utils.ScanUtil;

import java.util.ArrayList;
import java.util.List;

public class SScanCommand implements RedisCommand {
    @Override
    public RedisMessage execute(StorageEngine storage, RedisArray args, RedisContext context) {
        RedisMessage[] elements = args.elements();
        if (elements.length < 3) return errorArgs("sscan");

        String key = ((BulkString) elements[1]).asString();
        long cursor;
        try {
            cursor = Long.parseLong(((BulkString) elements[2]).asString());
        } catch (NumberFormatException e) {
            return new ErrorMessage("ERR invalid cursor");
        }

        // 1. 复用解析逻辑
        ScanUtil.ScanParams params;
        try {
            params = ScanUtil.parse(elements, 3);
        } catch (IllegalArgumentException e) {
            return new ErrorMessage(e.getMessage());
        }

        // 2. 获取数据
        RedisData<?> data = storage.get(key);
        if (data == null) return buildResponse("0", new ArrayList<>());
        if (data.getType() != RedisDataType.SET)
            return new ErrorMessage("WRONGTYPE Operation against a key holding the wrong kind of value");

        RedisSet set = data.getValue(RedisSet.class);
        List<byte[]> allMembers = set.getAll(); // O(N) Snapshot

        int size = allMembers.size();
        // 如果 cursor 已经越界，返回 0 和空列表
        if (cursor >= size) return buildResponse("0", new ArrayList<>());

        // 3. 分页与过滤
        List<byte[]> result = new ArrayList<>();
        // 注意：SCAN 的 COUNT 是 hint，不是硬限制。
        // 但对于这种 snapshot 实现，我们严格按照 count 返回。

        int scanCount = 0;
        long currentIdx = cursor;

        // 遍历直到收集够 count 个元素，或者遍历完列表
        // 注意：Redis SCAN 返回的 cursor 是下一次遍历的起始位置
        // 即使 MATCH 过滤掉了很多，cursor 也必须向前推进，
        // 否则客户端下次还会传回旧的 cursor，导致死循环。

        while (currentIdx < size && scanCount < params.count) {
            byte[] item = allMembers.get((int) currentIdx);
            if (params.match(item)) {
                result.add(item);
            }
            scanCount++; // 我们处理了这么多元素
            currentIdx++;
        }

        String nextCursor = (currentIdx >= size) ? "0" : String.valueOf(currentIdx);
        return buildResponse(nextCursor, result);
    }

    private RedisMessage buildResponse(String cursor, List<byte[]> members) {
        RedisMessage[] items = new RedisMessage[members.size()];
        for (int i = 0; i < members.size(); i++) {
            items[i] = new BulkString(members.get(i));
        }
        return new RedisArray(new RedisMessage[]{
                new BulkString(cursor),
                new RedisArray(items)
        });
    }
}
