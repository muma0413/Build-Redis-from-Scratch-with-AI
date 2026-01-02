package org.muma.mini.redis.protocol;

// 5. 数组 (*)
public record RedisArray(RedisMessage[] elements) implements RedisMessage {
}
