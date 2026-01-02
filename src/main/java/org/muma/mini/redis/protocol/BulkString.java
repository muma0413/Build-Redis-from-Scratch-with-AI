package org.muma.mini.redis.protocol;

import java.nio.charset.StandardCharsets;

// 4. 批量字符串 ($) - 支持 null (表示 $-1)
public record BulkString(byte[] content) implements RedisMessage {
    public BulkString(String s) {
        this(s == null ? null : s.getBytes(StandardCharsets.UTF_8));
    }

    public String asString() {
        return content == null ? null : new String(content, StandardCharsets.UTF_8);
    }
}
