package org.muma.mini.redis.command.impl.hash;

import org.muma.mini.redis.command.RedisCommand;
import org.muma.mini.redis.common.RedisData;
import org.muma.mini.redis.common.RedisDataType;
import org.muma.mini.redis.common.RedisHash;
import org.muma.mini.redis.protocol.*;
import org.muma.mini.redis.server.RedisContext;
import org.muma.mini.redis.store.StorageEngine;
import org.muma.mini.redis.utils.ScanUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * HSCAN key cursor [MATCH pattern] [COUNT count]
 * <p>
 * Time Complexity: O(N) (因为需要 toMap 全量拷贝)
 */
public class HScanCommand implements RedisCommand {
    @Override
    public RedisMessage execute(StorageEngine storage, RedisArray args, RedisContext context) {
        if (args.elements().length < 3) return errorArgs("hscan");

        String key = ((BulkString) args.elements()[1]).asString();
        long cursor;
        try {
            assert ((BulkString) args.elements()[2]).asString() != null;
            cursor = Long.parseLong(((BulkString) args.elements()[2]).asString());
        } catch (NumberFormatException e) {
            return new ErrorMessage("ERR invalid cursor");
        }

        // 1. 解析参数 (复用工具类)
        ScanUtil.ScanParams params;
        try {
            params = ScanUtil.parse(args.elements(), 3);
        } catch (IllegalArgumentException e) {
            return new ErrorMessage(e.getMessage());
        }

        // 2. 获取数据
        RedisData<?> data = storage.get(key);
        if (data == null) return buildResponse("0", new ArrayList<>());
        if (data.getType() != RedisDataType.HASH)
            return new ErrorMessage("WRONGTYPE Operation against a key holding the wrong kind of value");

        RedisHash hash = data.getValue(RedisHash.class);

        // 3. 转为列表以便分页 (性能瓶颈点)
        Map<String, byte[]> map = hash.toMap();
        List<Map.Entry<String, byte[]>> entries = new ArrayList<>(map.entrySet());

        int size = entries.size();
        if (cursor >= size) return buildResponse("0", new ArrayList<>());

        // 4. 扫描
        List<RedisMessage> result = new ArrayList<>();
        int scanCount = 0;
        long currentIdx = cursor;

        while (currentIdx < size && scanCount < params.count) {
            Map.Entry<String, byte[]> entry = entries.get((int) currentIdx);
            String field = entry.getKey();

            // 过滤
            if (params.match(field)) {
                result.add(new BulkString(field));
                result.add(new BulkString(entry.getValue()));
            }

            scanCount++;
            currentIdx++;
        }

        String nextCursor = (currentIdx >= size) ? "0" : String.valueOf(currentIdx);

        return new RedisArray(new RedisMessage[]{
                new BulkString(nextCursor),
                new RedisArray(result.toArray(new RedisMessage[0]))
        });
    }

    private RedisMessage buildResponse(String cursor, List<RedisMessage> list) {
        return new RedisArray(new RedisMessage[]{
                new BulkString(cursor),
                new RedisArray(list.toArray(new RedisMessage[0]))
        });
    }
}
