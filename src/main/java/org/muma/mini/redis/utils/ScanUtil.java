package org.muma.mini.redis.utils;

import org.muma.mini.redis.protocol.BulkString;
import org.muma.mini.redis.protocol.RedisMessage;

import java.nio.charset.StandardCharsets;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

public class ScanUtil {

    public static class ScanParams {
        public String matchPattern;
        public int count = 10; // Default
        public Pattern regex;  // Compiled regex (can be null)

        public boolean match(byte[] item) {
            if (regex == null) return true;
            String str = new String(item, StandardCharsets.UTF_8);
            return regex.matcher(str).matches();
        }

        // ZSet 需要匹配 String member
        public boolean match(String item) {
            if (regex == null) return true;
            return regex.matcher(item).matches();
        }
    }

    /**
     * 解析 [MATCH pattern] [COUNT count]
     *
     * @param elements   原始参数数组
     * @param startIndex 可选参数开始的索引 (SSCAN 是 3: key cursor ...)
     */
    public static ScanParams parse(RedisMessage[] elements, int startIndex) {
        ScanParams params = new ScanParams();

        for (int i = startIndex; i < elements.length; i += 2) {
            if (i + 1 >= elements.length) {
                throw new IllegalArgumentException("ERR syntax error");
            }
            String opt = ((BulkString) elements[i]).asString().toUpperCase();
            String val = ((BulkString) elements[i + 1]).asString();

            if ("MATCH".equals(opt)) {
                params.matchPattern = val;
                if (!"*".equals(val)) {
                    params.regex = convertGlobToRegex(val);
                }
            } else if ("COUNT".equals(opt)) {
                try {
                    params.count = Integer.parseInt(val);
                    if (params.count < 1) params.count = 10;
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("ERR value is not an integer or out of range");
                }
            } else {
                throw new IllegalArgumentException("ERR syntax error");
            }
        }
        return params;
    }

    private static Pattern convertGlobToRegex(String glob) {
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
            return Pattern.compile(".*");
        }
    }
}
