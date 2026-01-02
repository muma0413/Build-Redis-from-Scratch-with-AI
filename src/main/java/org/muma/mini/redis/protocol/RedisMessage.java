package org.muma.mini.redis.protocol;

import java.nio.charset.StandardCharsets;

// 密封接口，限制实现类
public sealed interface RedisMessage permits
        SimpleString, ErrorMessage, RedisInteger, BulkString, RedisArray {

    // 辅助方法：将字符串转为字节数组
    default byte[] toBytes(String content) {
        return content == null ? new byte[0] : content.getBytes(StandardCharsets.UTF_8);
    }
}





