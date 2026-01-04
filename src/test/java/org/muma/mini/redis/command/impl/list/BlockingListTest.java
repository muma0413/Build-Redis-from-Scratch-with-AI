package org.muma.mini.redis.command.impl.list;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.muma.mini.redis.common.RedisData;
import org.muma.mini.redis.protocol.BulkString;
import org.muma.mini.redis.protocol.RedisArray;
import org.muma.mini.redis.protocol.RedisMessage;
import org.muma.mini.redis.server.RedisContext;
import org.muma.mini.redis.store.StorageEngine;
import org.muma.mini.redis.store.impl.MemoryStorageEngine;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class BlockingListTest {

    private StorageEngine storage;
    private BRPopCommand brPop;
    private LPushCommand lPush;

    @BeforeEach
    void setUp() {
        storage = new MemoryStorageEngine();
        brPop = new BRPopCommand();
        lPush = new LPushCommand();
    }

    // 辅助方法：构造参数
    private RedisArray args(String... args) {
        RedisMessage[] msgs = new RedisMessage[args.length];
        for (int i = 0; i < args.length; i++) msgs[i] = new BulkString(args[i]);
        return new RedisArray(msgs);
    }

    /**
     * 测试场景：列表为空 -> 客户端阻塞 -> 另一个线程 Push -> 客户端收到数据并唤醒
     */
    @Test
    void testBRPopBlockingAndWakeup() throws InterruptedException {
        // 1. Mock 网络上下文
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        Channel channel = mock(Channel.class);
        when(ctx.channel()).thenReturn(channel);
        when(channel.isActive()).thenReturn(true); // 模拟连接活跃

        RedisContext context = new RedisContext(ctx);

        // 2. 启动一个线程模拟客户端 A 执行 BRPOP list1 5 (等待5秒)
        CountDownLatch blockLatch = new CountDownLatch(1);

        new Thread(() -> {
            try {
                // 执行 BRPOP，预期返回 null (进入阻塞状态)
                RedisMessage res = brPop.execute(storage, args("BRPOP", "list1", "5"), context);
                assertNull(res, "BRPOP should return null immediately when blocking");

                blockLatch.countDown(); // 告诉主线程：我已经进入阻塞了
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();

        // 等待子线程进入阻塞状态
        assertTrue(blockLatch.await(1, TimeUnit.SECONDS));

        // 验证：此时还没有任何输出
        verify(ctx, never()).writeAndFlush(any());

        // 3. 主线程模拟客户端 B 执行 LPUSH list1 "hello"
        // 这应该触发 BlockingManager 的 onPush
        Thread.sleep(100); // 稍微等一下，确保 addWait 已完成
        lPush.execute(storage, args("LPUSH", "list1", "hello"), new RedisContext(mock(ChannelHandlerContext.class)));

        // 4. 验证：客户端 A 的 ctx 应该收到了 writeAndFlush
        // 捕获输出参数
        ArgumentCaptor<RedisMessage> captor = ArgumentCaptor.forClass(RedisMessage.class);
        // 给一点时间让异步线程执行回调
        verify(ctx, timeout(1000).times(1)).writeAndFlush(captor.capture());

        // 5. 检查收到的数据是否正确
        // 预期格式: Array [BulkString("list1"), BulkString("hello")]
        RedisMessage response = captor.getValue();
        assertTrue(response instanceof RedisArray);
        RedisMessage[] elements = ((RedisArray) response).elements();
        assertEquals(2, elements.length);
        assertEquals("list1", ((BulkString) elements[0]).asString());
        assertEquals("hello", ((BulkString) elements[1]).asString());

        // 6. 验证 List 是否被 Pop 空了 (RPOP 弹出刚 push 的一个)
        RedisData<?> data = storage.get("list1");
        assertNull(data, "List should be empty and removed after pop");
    }

    /**
     * 测试场景：列表已有数据 -> BRPOP 直接返回不阻塞
     */
    @Test
    void testBRPopNonBlocking() {
        // 先 Push 数据
        lPush.execute(storage, args("LPUSH", "list1", "val1"), new RedisContext(null));

        // 执行 BRPOP
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        RedisMessage res = brPop.execute(storage, args("BRPOP", "list1", "1"), new RedisContext(ctx));

        // 预期：立即返回，不阻塞，不返回 null
        assertNotNull(res);
        assertTrue(res instanceof RedisArray);
        assertEquals("val1", ((BulkString)((RedisArray)res).elements()[1]).asString());

        // 验证没有注册阻塞
        verify(ctx, never()).writeAndFlush(any()); // 只有阻塞回调才会调这个，直接返回则由 Handler 处理
    }
}
