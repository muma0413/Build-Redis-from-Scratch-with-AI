package org.muma.mini.redis.protocol;

// 3. 整数 (:)
public record RedisInteger(long value) implements RedisMessage {
}
