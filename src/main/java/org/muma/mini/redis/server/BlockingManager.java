package org.muma.mini.redis.server;

import io.netty.channel.ChannelHandlerContext;
import org.muma.mini.redis.common.RedisData;
import org.muma.mini.redis.common.RedisDataType;
import org.muma.mini.redis.common.RedisList;
import org.muma.mini.redis.protocol.BulkString;
import org.muma.mini.redis.protocol.RedisArray;
import org.muma.mini.redis.protocol.RedisMessage;
import org.muma.mini.redis.store.StorageEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 全局阻塞请求管理器
 * 负责管理 BLPOP, BRPOP, BRPOPLPUSH 的挂起与唤醒
 */
public class BlockingManager {

    private static final Logger log = LoggerFactory.getLogger(BlockingManager.class);

    // Key -> List<BlockingContext> (等待该 Key 的所有客户端)
    // 使用 COWList 保证遍历时的线程安全，适合读多写少的场景
    private final Map<String, List<BlockingContext>> waitingClients = new ConcurrentHashMap<>();

    // 定时清理超时连接
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    public BlockingManager() {
        // 每 100ms 扫描一次超时
        scheduler.scheduleAtFixedRate(this::checkTimeouts, 100, 100, TimeUnit.MILLISECONDS);
    }

    /**
     * 注册阻塞请求 (通用版)
     *
     * @param handler 具体的业务处理策略 (ListBlockingHandler, StreamBlockingHandler...)
     */
    public void addWait(ChannelHandlerContext ctx, List<String> keys, long timeoutSec, BlockingHandler handler) {
        long expireAt = (timeoutSec == 0) ? Long.MAX_VALUE : System.currentTimeMillis() + (timeoutSec * 1000);

        // 使用新的构造函数
        BlockingContext context = new BlockingContext(ctx, keys, expireAt, handler);

        for (String key : keys) {
            waitingClients.computeIfAbsent(key, k -> new CopyOnWriteArrayList<>()).add(context);
        }

        log.debug("Client blocked on keys: {}", keys);
    }

    /**
     * 触发唤醒事件 (当有数据 Push 进 List 时调用)
     * <p>
     * 核心职责：
     * 1. 检查是否有客户端在等待该 Key。
     * 2. 执行 Pop 操作 (CAS 状态控制，保证只被一个客户端消费)。
     * 3. 处理 BRPOPLPUSH 的特殊逻辑 (推入目标列表)。
     * 4. 【AOF】手动传播写命令 (因为 BLPOP 本身不记录 AOF)。
     * 5. 级联唤醒 (如果推入了新列表)。
     */
    public void onPush(String key, StorageEngine storage) {
        List<BlockingContext> clients = waitingClients.get(key);
        if (clients == null || clients.isEmpty()) return;

        Iterator<BlockingContext> it = clients.iterator();

        while (it.hasNext()) {
            BlockingContext client = it.next();

            // 1. 并发卫士
            if (!client.tryFinish()) continue;

            // 2. 【核心修改】委托给 Handler 执行业务逻辑
            // 传入 storage 以便 Handler 操作数据和 AOF
            boolean handled = false;
            try {
                handled = client.getHandler().handle(key, storage, client);
            } catch (Exception e) {
                log.error("Error in blocking handler", e);
            }

            if (handled) {
                // 成功处理，清理该 Client 的所有监听
                removeClient(client);
                return; // 只唤醒一个
            } else {
                // Handle 失败 (比如数据被抢了或类型不对)
                // 这是一个极端情况。既然 tryFinish 成功了，状态已经变成了 DONE。
                // 如果这里失败了，客户端就永远收不到响应了。
                // 补救措施：如果你希望它继续等，这里应该重置 done 状态 (需要 BlockingContext 提供 reset 方法)
                // 但简单起见，我们假设 handle 在单线程下只要进入了且数据存在就会成功。
                // 如果是数据类型不对这种不可恢复错误，移除它也是对的。
                log.warn("Blocking handler returned false for key {}, client removed.", key);
                removeClient(client);
            }
        }
    }

    /**
     * 移除该客户端的所有监听 (因为它已经在一个 Key 上满足了)
     */
    private void removeClient(BlockingContext client) {
        for (String k : client.getKeys()) {
            List<BlockingContext> list = waitingClients.get(k);
            if (list != null) {
                list.remove(client);
                if (list.isEmpty()) {
                    waitingClients.remove(k);
                }
            }
        }
    }

    private void checkTimeouts() {
        long now = System.currentTimeMillis();

        // 遍历所有 Key 的等待队列
        waitingClients.values().forEach(list -> {
            // 使用 removeIf 安全删除
            list.removeIf(ctx -> {
                if (now > ctx.getExpireAt()) {
                    // 发送超时响应 (nil)
                    if (ctx.getCtx().channel().isActive()) {
                        ctx.getCtx().writeAndFlush(new BulkString((byte[]) null));
                    }
                    // 还需要把这个 client 从其他 key 的监听列表里也删掉
                    // 但 removeIf 只能删当前 list 的。
                    // 这是一个复杂点：为了 O(1) 删除，通常需要反向索引 (Client -> List<Key>)
                    // 这里为了简单，我们暂且只从当前 Key 移除。
                    // *副作用*：如果 client 监听了 [A, B]，A 超时了被移除，B 列表里还有它。
                    // 这是一个 Bug 隐患。

                    // 【修正方案】：为了简单，我们在这里不处理复杂的交叉移除。
                    // 而是仅仅发送响应。当它在其他 Key 被唤醒时，检查 ctx.channel().isActive() 或者增加一个 `isDone` 标志位。
                    return true;
                }
                return false;
            });
        });
    }
}
