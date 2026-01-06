package org.muma.mini.redis.command;

import io.netty.channel.ChannelHandlerContext;
import org.muma.mini.redis.common.RedisZSet;
import org.muma.mini.redis.protocol.BulkString;
import org.muma.mini.redis.protocol.ErrorMessage;
import org.muma.mini.redis.protocol.RedisArray;
import org.muma.mini.redis.protocol.RedisMessage;
import org.muma.mini.redis.server.RedisContext;
import org.muma.mini.redis.store.StorageEngine;

import java.util.List;

public interface RedisCommand {
    // 执行命令，传入存储引擎和参数
    RedisMessage execute(StorageEngine storage, RedisArray args, RedisContext context);


    /**
     * ZSet 响应构建工具 (Default Method)
     * 允许 ZRange, ZRevRange 等命令直接复用，无需额外的 Utils 类
     */
    default RedisMessage buildZSetResponse(List<RedisZSet.ZSetEntry> list, boolean withScores) {
        int size = list.size() * (withScores ? 2 : 1);
        RedisMessage[] result = new RedisMessage[size];
        int i = 0;
        for (RedisZSet.ZSetEntry entry : list) {
            result[i++] = new BulkString(entry.member());
            if (withScores) {
                // 浮点数格式化：去掉整数后面的 .0
                double s = entry.score();
                String scoreStr = (s % 1 == 0) ?
                        String.valueOf((long) s) : String.valueOf(s);
                result[i++] = new BulkString(scoreStr);
            }
        }
        return new RedisArray(result);
    }

    /**
     * 辅助工具：快速构建参数错误
     * (未来很多命令都会用到)
     */
    default ErrorMessage errorArgs(String cmd) {
        return new ErrorMessage("ERR wrong number of arguments for '" + cmd + "' command");
    }

    /**
     * 辅助工具：快速构建数值错误
     */
    default ErrorMessage errorInt() {
        return new ErrorMessage("ERR value is not an integer or out of range");
    }


    // 默认不是写命令，所有 SET/HSET 等需要覆盖返回 true
    default boolean isWrite() {
        return false;
    }

}
