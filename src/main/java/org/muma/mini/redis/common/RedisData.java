package org.muma.mini.redis.common;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@NoArgsConstructor
public class RedisData<T> implements Serializable {

    // 数据类型
    private RedisDataType type;

    // 过期时间 (-1 表示不过期)
    private long expireAt = -1;

    // 泛型数据载体 (String是byte[], Hash是RedisHash对象, List是LinkedList等)
    private T data;

    public RedisData(RedisDataType type, T data) {
        this.type = type;
        this.data = data;
    }

    public boolean isExpired() {
        return expireAt != -1 && System.currentTimeMillis() > expireAt;
    }

    // 这是一个非常实用的辅助方法，避免外部强制转换时报 Unchecked warning
    // 同时也方便做类型检查
    public <V> V getValue(Class<V> clazz) {
        if (clazz.isInstance(data)) {
            return clazz.cast(data);
        }
        throw new IllegalStateException("Data type mismatch. Expected " + clazz.getSimpleName() + " but found " + data.getClass().getSimpleName());
    }
}
