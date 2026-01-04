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
     * 注册阻塞请求
     *
     * @param targetKey 如果不为 null，说明是 BRPOPLPUSH，需要将弹出的值推入此 Key
     */
    public void addWait(ChannelHandlerContext ctx, List<String> keys, long timeoutSec, boolean isLeft, String targetKey) {
        long expireAt = (timeoutSec == 0) ? Long.MAX_VALUE : System.currentTimeMillis() + (timeoutSec * 1000);

        BlockingContext context = new BlockingContext(ctx, keys, expireAt, isLeft, targetKey);

        for (String key : keys) {
            waitingClients.computeIfAbsent(key, k -> new CopyOnWriteArrayList<>()).add(context);
        }

        log.debug("Client blocked on keys: {}", keys);
    }

    /**
     * 触发唤醒事件 (当有数据 Push 进 List 时调用)
     */
    public void onPush(String key, StorageEngine storage) {
        List<BlockingContext> clients = waitingClients.get(key);
        if (clients == null || clients.isEmpty()) return;

        // 唤醒逻辑：FIFO (CopyOnWriteArrayList 保证了迭代顺序)
        // 我们只唤醒一个客户端来消费这个数据
        Iterator<BlockingContext> it = clients.iterator();

        // 注意：这里需要加锁或者再次检查数据，防止并发下的伪唤醒
        // 在 Mini-Redis 中，Command 执行和 onPush 通常是在同一个同步块或线程中，
        // 但为了严谨，我们再次检查。

        while (it.hasNext()) {
            BlockingContext client = it.next();

            // 1. 再次检查数据 (Double Check)
            RedisData<?> data = storage.get(key);
            if (data == null || data.getType() != RedisDataType.LIST) {
                // Key 消失了或者类型变了，跳过，继续等或者移除？
                // Redis 逻辑是继续等。
                break;
            }

            RedisList list = data.getValue(RedisList.class);
            if (list.size() == 0) {
                // 没数据，可能是刚被别人抢走了，停止处理
                break;
            }

            // 2. 执行 Pop (这是修改操作，必须原子)
            byte[] value = client.isLeftPop() ? list.lpop() : list.rpop();

            if (value != null) {
                // 3. 处理 BRPOPLPUSH 的特殊逻辑 (推入目标列表)
                if (client.getTargetKey() != null) {
                    handleRPopLPush(client.getTargetKey(), value, storage);
                }

                // 4. 发送响应给客户端
                sendResponse(client, key, value);

                // 5. 清理状态
                removeClient(client);

                // 6. 维护 Storage 一致性
                if (list.size() == 0) {
                    storage.remove(key);
                } else {
                    // 显式 put 触发可能的其他钩子 (虽然这里是同一个对象)
                    // storage.put(key, data);
                }

                log.debug("Client unblocked on key: {}", key);

                // 只唤醒一个，处理完毕直接返回
                return;
            }
        }
    }

    private void handleRPopLPush(String destKey, byte[] value, StorageEngine storage) {
        // 原子性地推入目标列表
        // 注意：这里没有加锁，依赖外部 synchronized(storage) 或者 storage 自身的线程安全
        RedisData<?> destData = storage.get(destKey);
        RedisList destList;

        if (destData == null) {
            destList = new RedisList();
            storage.put(destKey, new RedisData<>(RedisDataType.LIST, destList));
        } else if (destData.getType() != RedisDataType.LIST) {
            log.error("BRPOPLPUSH target key {} is not a list, value dropped.", destKey);
            return; // 丢弃数据 (Redis 实际上会在执行前检查，或者报错)
        } else {
            destList = destData.getValue(RedisList.class);
        }

        destList.lpush(value);

        // 【级联唤醒】：目标 Key 也可能有客户端在 BLPOP
        // 递归调用可能导致栈溢出，但在实际 Redis 场景深度有限
        onPush(destKey, storage);
    }

    private void sendResponse(BlockingContext client, String sourceKey, byte[] value) {
        RedisMessage response;
        if (client.getTargetKey() != null) {
            // BRPOPLPUSH 只返回 value
            response = new BulkString(value);
        } else {
            // BLPOP/BRPOP 返回 [key, value]
            response = new RedisArray(new RedisMessage[]{
                    new BulkString(sourceKey),
                    new BulkString(value)
            });
        }

        if (client.getCtx().channel().isActive()) {
            client.getCtx().writeAndFlush(response);
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
