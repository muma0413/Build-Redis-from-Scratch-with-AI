package org.muma.mini.redis.server;

import io.netty.channel.ChannelHandlerContext;

/**
 * 命令执行上下文
 * 封装了与当前连接相关的所有环境信息
 */
public class RedisContext {

    private final ChannelHandlerContext nettyCtx;
    // 未来可扩展:
    // private int dbIndex;
    // private User currentUser;
    // private boolean inTransaction;

    public RedisContext(ChannelHandlerContext nettyCtx) {
        this.nettyCtx = nettyCtx;
    }

    public ChannelHandlerContext getNettyCtx() {
        return nettyCtx;
    }
}
