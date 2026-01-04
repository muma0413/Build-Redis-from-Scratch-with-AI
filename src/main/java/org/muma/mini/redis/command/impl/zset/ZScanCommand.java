package org.muma.mini.redis.command.impl.zset;

import org.muma.mini.redis.command.RedisCommand;
import org.muma.mini.redis.common.RedisData;
import org.muma.mini.redis.common.RedisDataType;
import org.muma.mini.redis.common.RedisZSet;
import org.muma.mini.redis.protocol.*;
import org.muma.mini.redis.server.RedisContext;
import org.muma.mini.redis.store.StorageEngine;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * ZSCAN key cursor [MATCH pattern] [COUNT count]
 * <p>
 * Time Complexity: O(log(N) + M)
 * M 是 COUNT 的大小，通常很小 (默认10)。
 * <p>
 * 【高性能设计】
 * 不同于 Hash 表的 SCAN 需要复杂的反向二进制迭代，
 * ZSet 本身是有序的 (SkipList/ZipList)。
 * 我们直接使用基于 Rank (索引) 的分页查询来实现 SCAN。
 * <p>
 * 优点：
 * 1. 绝对非阻塞。
 * 2. 遍历结果是有序的 (按 Score)。
 */
public class ZScanCommand implements RedisCommand {
    @Override
    public RedisMessage execute(StorageEngine storage,RedisArray args, RedisContext context) {
        RedisMessage[] elements = args.elements();
        if (elements.length < 3) {
            return errorArgs("zscan");
        }

        String key = ((BulkString) elements[1]).asString();
        long cursor;
        try {
            assert ((BulkString) elements[2]).asString() != null;
            cursor = Long.parseLong(((BulkString) elements[2]).asString());
        } catch (NumberFormatException e) {
            return new ErrorMessage("ERR invalid cursor");
        }

        // --- 1. 解析可选参数 MATCH, COUNT ---
        String matchPattern = null;
        int count = 10; // 默认 Count

        for (int i = 3; i < elements.length; i += 2) {
            if (i + 1 >= elements.length) return new ErrorMessage("ERR syntax error");
            assert ((BulkString) elements[i]).asString() != null;
            String opt = ((BulkString) elements[i]).asString().toUpperCase();
            String val = ((BulkString) elements[i + 1]).asString();

            if ("MATCH".equals(opt)) {
                matchPattern = val;
            } else if ("COUNT".equals(opt)) {
                try {
                    assert val != null;
                    count = Integer.parseInt(val);
                    if (count < 1) count = 10; // 保护性默认值
                } catch (NumberFormatException e) {
                    return new ErrorMessage("ERR value is not an integer or out of range");
                }
            } else {
                return new ErrorMessage("ERR syntax error");
            }
        }

        // --- 2. 获取数据 ---
        // SCAN 即使 Key 不存在也不报错，而是返回 0 和空列表
        RedisData<?> data = storage.get(key);
        if (data == null) {
            return buildScanResponse("0", new ArrayList<>());
        }
        if (data.getType() != RedisDataType.ZSET) {
            return new ErrorMessage("WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        RedisZSet zset = data.getValue(RedisZSet.class);
        int totalSize = zset.size();

        // --- 3. 执行扫描 (核心逻辑) ---
        // cursor 在这里被视为 0-based index
        if (cursor >= totalSize) {
            return buildScanResponse("0", new ArrayList<>());
        }

        long start = cursor;
        long stop = cursor + count - 1;

        // 利用现有的 range 方法 (O(logN + M))
        List<RedisZSet.ZSetEntry> rawList = zset.range(start, stop);

        // --- 4. 过滤 (MATCH) ---
        List<RedisZSet.ZSetEntry> filteredList;
        if (matchPattern == null || matchPattern.equals("*")) {
            filteredList = rawList;
        } else {
            filteredList = new ArrayList<>();
            Pattern regex = convertGlobToRegex(matchPattern);
            for (RedisZSet.ZSetEntry entry : rawList) {
                if (regex.matcher(entry.member()).matches()) {
                    filteredList.add(entry);
                }
            }
        }

        // --- 5. 计算下一个游标 ---
        // 如果本次读取已经到达或超过末尾，游标归零
        long nextCursor = (stop >= totalSize - 1) ? 0 : (stop + 1);

        return buildScanResponse(String.valueOf(nextCursor), filteredList);
    }

    /**
     * 构建 ZSCAN 的标准返回格式:
     * [ "next_cursor", [member, score, member, score...] ]
     */
    private RedisMessage buildScanResponse(String nextCursor, List<RedisZSet.ZSetEntry> list) {
        // 1. 游标部分
        RedisMessage cursorMsg = new BulkString(nextCursor);

        // 2. 数据部分 (ZSCAN 强制带分数 WITHSCORES)
        // 复用父接口的 buildZSetResponse，强制 withScores=true
        RedisMessage dataMsg = buildZSetResponse(list, true);

        return new RedisArray(new RedisMessage[]{cursorMsg, dataMsg});
    }

    /**
     * 简单的 Glob 转 Regex 工具 (用于 MATCH)
     * 支持 * 和 ?
     */
    private Pattern convertGlobToRegex(String glob) {
        StringBuilder sb = new StringBuilder("^");
        for (char c : glob.toCharArray()) {
            switch (c) {
                case '*':
                    sb.append(".*");
                    break;
                case '?':
                    sb.append(".");
                    break;
                case '.':
                    sb.append("\\.");
                    break;
                case '\\':
                    sb.append("\\\\");
                    break;
                case '{':
                    sb.append("\\{");
                    break;
                case '}':
                    sb.append("\\}");
                    break;
                case '(':
                    sb.append("\\(");
                    break;
                case ')':
                    sb.append("\\)");
                    break;
                case '[':
                    sb.append("\\[");
                    break;
                case ']':
                    sb.append("\\]");
                    break;
                case '+':
                    sb.append("\\+");
                    break;
                case '^':
                    sb.append("\\^");
                    break;
                case '$':
                    sb.append("\\$");
                    break;
                default:
                    sb.append(c);
            }
        }
        sb.append("$");
        try {
            return Pattern.compile(sb.toString());
        } catch (PatternSyntaxException e) {
            return Pattern.compile(".*"); // Fallback
        }
    }
}
