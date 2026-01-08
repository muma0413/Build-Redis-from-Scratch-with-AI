package org.muma.mini.redis.server;

import io.netty.channel.ChannelHandlerContext;
import lombok.Getter;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 阻塞上下文 (通用版)
 * 记录客户端的连接信息、监听的 Keys、超时设置以及具体的处理策略。
 *
 * 职责：
 * 1. 持有上下文信息 (Ctx, Keys, Timeout)
 * 2. 持有策略对象 (Handler)
 * 3. 并发状态控制 (AtomicBoolean done)
 */
public class BlockingContext {

    @Getter
    private final ChannelHandlerContext ctx;

    @Getter
    private final List<String> keys;

    @Getter
    private final long expireAt;

    // 【核心修改】持有策略接口，不再持有具体的 List 参数
    @Getter
    private final BlockingHandler handler;

    // 核心状态位：是否已经完成（无论是被唤醒还是超时）
    private final AtomicBoolean done = new AtomicBoolean(false);

    /**
     * 构造函数
     * @param ctx Netty 上下文
     * @param keys 监听的 Key 列表
     * @param expireAt 超时时间戳
     * @param handler 具体的唤醒/超时处理逻辑
     */
    public BlockingContext(ChannelHandlerContext ctx, List<String> keys, long expireAt, BlockingHandler handler) {
        this.ctx = ctx;
        this.keys = keys;
        this.expireAt = expireAt;
        this.handler = handler;
    }

    /**
     * 尝试完成此次阻塞请求。
     * 使用 CAS (Compare-And-Set) 保证线程安全。
     *
     * @return true if successful (first winner);
     *         false if already done.
     */
    public boolean tryFinish() {
        return done.compareAndSet(false, true);
    }
}
