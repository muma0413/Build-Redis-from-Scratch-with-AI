package org.muma.mini.redis.server;

import io.netty.util.concurrent.DefaultEventExecutor;
import io.netty.util.concurrent.EventExecutor;

/**
 * 核心业务线程 (Single Thread Logic)
 * 所有的 Command.execute 都在这里排队执行。
 * 实现了无锁化。
 */
public class RedisCoreExecutor {

    // 使用 Netty 的 DefaultEventExecutor，它是一个高效的单线程事件循环
    private final EventExecutor singleThread = new DefaultEventExecutor();

    public void submit(Runnable task) {
        singleThread.submit(task);
    }
}
