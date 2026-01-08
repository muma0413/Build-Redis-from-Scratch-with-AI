package org.muma.mini.redis.server;

import org.muma.mini.redis.store.StorageEngine;

/**
 * 阻塞事件处理器
 * 定义当 Key 就绪时具体的业务行为
 */
public interface BlockingHandler {

    /**
     * 尝试处理唤醒事件
     *
     * @param key     被触发的 Key
     * @param storage 存储引擎
     * @return true if handled successfully (should remove listener), false otherwise
     */
    boolean handle(String key, StorageEngine storage, BlockingContext context);

    /**
     * 当超时发生时的回调
     */
    void onTimeout(BlockingContext context);
}
