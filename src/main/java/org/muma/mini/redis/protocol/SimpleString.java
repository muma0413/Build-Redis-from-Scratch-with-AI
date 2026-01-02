package org.muma.mini.redis.protocol;

// 1. 简单字符串 (+)
public record SimpleString(String content) implements RedisMessage {
}
