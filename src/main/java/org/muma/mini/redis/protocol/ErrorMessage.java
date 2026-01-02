package org.muma.mini.redis.protocol;

// 2. 错误信息 (-)
public record ErrorMessage(String content) implements RedisMessage {
}
