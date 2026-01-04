package org.muma.mini.redis.server;

import io.netty.channel.ChannelHandlerContext;
import lombok.Getter;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 阻塞上下文
 * 记录客户端的连接信息、监听的 Keys、超时设置等。
 * <p>
 * 【从 Record 改为 Class 的原因】
 * 我们需要一个可变的状态位 (done) 来解决 "超时" 和 "唤醒" 之间的并发竞争。
 * 如果一个客户端监听了多个 Key，或者同时面临超时，必须保证只响应一次。
 */
public class BlockingContext {

    @Getter
    private final ChannelHandlerContext ctx;
    @Getter
    private final List<String> keys;
    @Getter
    private final long expireAt;
    @Getter
    private final boolean isLeftPop;
    @Getter
    private final String targetKey; // 用于 BRPOPLPUSH

    // 核心状态位：是否已经完成（无论是被唤醒还是超时）
    private final AtomicBoolean done = new AtomicBoolean(false);

    public BlockingContext(ChannelHandlerContext ctx, List<String> keys, long expireAt, boolean isLeftPop, String targetKey) {
        this.ctx = ctx;
        this.keys = keys;
        this.expireAt = expireAt;
        this.isLeftPop = isLeftPop;
        this.targetKey = targetKey;
    }

    /**
     * 尝试完成此次阻塞请求。
     * 使用 CAS (Compare-And-Set) 保证线程安全。
     *
     * @return true 如果成功抢占状态（说明是第一次完成）；
     * false 如果已经被处理过（无论是超时还是被其他 Key 唤醒）。
     */
    public boolean tryFinish() {
        return done.compareAndSet(false, true);
    }
}
