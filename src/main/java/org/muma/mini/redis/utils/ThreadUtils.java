package org.muma.mini.redis.utils;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 线程工厂与管理工具
 */
public class ThreadUtils {

    public static ThreadFactory namedThreadFactory(String prefix) {
        return new RedisThreadFactory(prefix);
    }

    private static class RedisThreadFactory implements ThreadFactory {
        private final String prefix;
        private final AtomicInteger counter = new AtomicInteger(1);

        public RedisThreadFactory(String prefix) {
            this.prefix = prefix;
        }

        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r, prefix + "-" + counter.getAndIncrement());
            t.setDaemon(true); // 默认后台线程，防止阻塞 JVM 关闭
            return t;
        }
    }
}
