package org.muma.mini.redis.command.impl.list;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.muma.mini.redis.common.RedisData;
import org.muma.mini.redis.protocol.*;
import org.muma.mini.redis.server.RedisContext;
import org.muma.mini.redis.store.StorageEngine;
import org.muma.mini.redis.store.impl.MemoryStorageEngine;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class ListBlockingTest {

    private StorageEngine storage;
    private BLPopCommand blpop;
    private LPushCommand lpush;

    @BeforeEach
    void setUp() {
        storage = new MemoryStorageEngine();
        blpop = new BLPopCommand();
        lpush = new LPushCommand();
    }

    private RedisArray args(String... args) {
        RedisMessage[] msgs = new RedisMessage[args.length];
        for (int i = 0; i < args.length; i++) msgs[i] = new BulkString(args[i]);
        return new RedisArray(msgs);
    }

    /**
     * 测试：阻塞直到数据推入
     */
    @Test
    void testBlockingPop() throws InterruptedException {
        // 1. Mock 网络环境
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        Channel channel = mock(Channel.class);
        when(ctx.channel()).thenReturn(channel);
        when(channel.isActive()).thenReturn(true);
        RedisContext context = new RedisContext(ctx);

        CountDownLatch latch = new CountDownLatch(1);

        // 2. 启动消费者线程 (BLPOP)
        new Thread(() -> {
            // BLPOP list 5
            RedisMessage res = blpop.execute(storage, args("BLPOP", "list", "5"), context);
            assertNull(res, "Blocking command should return null immediately");
            latch.countDown();
        }).start();

        // 等待消费者进入阻塞状态
        latch.await(1, TimeUnit.SECONDS);

        // 3. 生产者推入数据 (LPUSH)
        // 模拟另一个客户端上下文 (不需要 Mock ctx)
        lpush.execute(storage, args("LPUSH", "list", "val"), new RedisContext(null));

        // 4. 验证消费者收到了响应
        // 等待异步通知 (Mockito verify 会等待)
        ArgumentCaptor<RedisMessage> captor = ArgumentCaptor.forClass(RedisMessage.class);
        verify(ctx, timeout(1000).times(1)).writeAndFlush(captor.capture());

        RedisMessage msg = captor.getValue();
        assertTrue(msg instanceof RedisArray);
        RedisMessage[] els = ((RedisArray) msg).elements();
        assertEquals("list", ((BulkString)els[0]).asString());
        assertEquals("val", ((BulkString)els[1]).asString());
    }

    /**
     * 测试：阻塞超时
     */
    @Test
    void testBlockingTimeout() throws InterruptedException {
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        Channel channel = mock(Channel.class);
        when(ctx.channel()).thenReturn(channel);
        when(channel.isActive()).thenReturn(true);
        RedisContext context = new RedisContext(ctx);

        // BLPOP list 1 (1秒超时)
        blpop.execute(storage, args("BLPOP", "list", "1"), context);

        // 验证没有立即收到消息
        verify(ctx, never()).writeAndFlush(any());

        // 等待超时 (稍微多等一点)
        Thread.sleep(1500);

        // 验证收到了超时消息 (nil)
        ArgumentCaptor<RedisMessage> captor = ArgumentCaptor.forClass(RedisMessage.class);
        verify(ctx).writeAndFlush(captor.capture());

        RedisMessage msg = captor.getValue();
        assertTrue(msg instanceof BulkString);
        assertNull(((BulkString)msg).content()); // nil
    }

    /**
     * 测试：BRPOP (右侧弹出)
     */
    @Test
    void testBRPop() throws InterruptedException {
        // Mock 略... (同 BLPOP)
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        Channel channel = mock(Channel.class);
        when(ctx.channel()).thenReturn(channel);
        when(channel.isActive()).thenReturn(true);
        RedisContext context = new RedisContext(ctx);
        CountDownLatch latch = new CountDownLatch(1);

        BRPopCommand brpop = new BRPopCommand();

        new Thread(() -> {
            brpop.execute(storage, args("BRPOP", "list", "5"), context);
            latch.countDown();
        }).start();

        latch.await(1, TimeUnit.SECONDS);

        // RPUSH 推入 [a, b]
        RPushCommand rpush = new RPushCommand();
        rpush.execute(storage, args("RPUSH", "list", "a", "b"), new RedisContext(null));

        // BRPOP 应该弹出 b (尾部)
        ArgumentCaptor<RedisMessage> captor = ArgumentCaptor.forClass(RedisMessage.class);
        verify(ctx, timeout(1000).times(1)).writeAndFlush(captor.capture());

        RedisMessage[] els = ((RedisArray) captor.getValue()).elements();
        assertEquals("b", ((BulkString)els[1]).asString());
    }

    /**
     * 测试：BRPOPLPUSH (阻塞式 弹+推)
     * 场景：源为空 -> 阻塞 -> 源推入 -> 唤醒(弹源推目标)
     */
    @Test
    void testBRPopLPush() throws InterruptedException {
        // Mock ...
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        Channel channel = mock(Channel.class);
        when(ctx.channel()).thenReturn(channel);
        when(channel.isActive()).thenReturn(true);
        RedisContext context = new RedisContext(ctx);
        CountDownLatch latch = new CountDownLatch(1);

        BRPopLPushCommand brpoplpush = new BRPopLPushCommand();

        new Thread(() -> {
            // BRPOPLPUSH src dest 5
            brpoplpush.execute(storage, args("BRPOPLPUSH", "src", "dest", "5"), context);
            latch.countDown();
        }).start();

        latch.await(1, TimeUnit.SECONDS);

        // 往 src 推入 "val"
        new LPushCommand().execute(storage, args("LPUSH", "src", "val"), new RedisContext(null));

        // 验证 1: 客户端收到了 "val" (BulkString, 不是 Array)
        ArgumentCaptor<RedisMessage> captor = ArgumentCaptor.forClass(RedisMessage.class);
        verify(ctx, timeout(1000).times(1)).writeAndFlush(captor.capture());

        RedisMessage res = captor.getValue();
        assertTrue(res instanceof BulkString);
        assertEquals("val", ((BulkString)res).asString());

        // 验证 2: dest 列表里有了 "val"
        RedisData<?> destData = storage.get("dest");
        assertNotNull(destData);
        assertEquals(1, ((org.muma.mini.redis.common.RedisList)destData.getData()).size());
    }
}
