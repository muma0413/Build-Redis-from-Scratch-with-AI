package org.muma.mini.redis.server.handler;

import io.netty.channel.ChannelHandlerContext;
import org.muma.mini.redis.common.RedisData;
import org.muma.mini.redis.common.RedisDataType;
import org.muma.mini.redis.common.RedisList;
import org.muma.mini.redis.protocol.*;
import org.muma.mini.redis.server.BlockingContext;
import org.muma.mini.redis.server.BlockingHandler;
import org.muma.mini.redis.store.StorageEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ListBlockingHandler implements BlockingHandler {

    private static final Logger log = LoggerFactory.getLogger(ListBlockingHandler.class);

    private final boolean isLeftPop;
    private final String targetKey; // for BRPOPLPUSH

    public ListBlockingHandler(boolean isLeftPop, String targetKey) {
        this.isLeftPop = isLeftPop;
        this.targetKey = targetKey;
    }

    @Override
    public boolean handle(String key, StorageEngine storage, BlockingContext context) {
        // 1. 检查数据
        RedisData<?> data = storage.get(key);
        if (data == null || data.getType() != RedisDataType.LIST) return false;

        RedisList list = data.getValue(RedisList.class);
        if (list.size() == 0) return false;

        // 2. 执行 Pop
        byte[] value = isLeftPop ? list.lpop() : list.rpop();
        if (value == null) return false;

        // 3. AOF (源)
        String popCmd = isLeftPop ? "LPOP" : "RPOP";
        storage.appendAof(new RedisArray(new RedisMessage[]{ new BulkString(popCmd), new BulkString(key) }));

        // 4. BRPOPLPUSH 逻辑
        if (targetKey != null) {
            handleTargetPush(targetKey, value, storage);
        }

        // 5. 回包
        sendResponse(context.getCtx(), key, value);

        // 6. 清理 Storage
        if (list.size() == 0) storage.remove(key);
        else storage.put(key, data);

        return true;
    }

    @Override
    public void onTimeout(BlockingContext context) {
        if (context.getCtx().channel().isActive()) {
            context.getCtx().writeAndFlush(new BulkString((byte[]) null));
        }
    }

    private void handleTargetPush(String destKey, byte[] value, StorageEngine storage) {
        RedisData<?> destData = storage.get(destKey);
        RedisList destList;
        if (destData == null) {
            destList = new RedisList();
            storage.put(destKey, new RedisData<>(RedisDataType.LIST, destList));
        } else if (destData.getType() == RedisDataType.LIST) {
            destList = destData.getValue(RedisList.class);
        } else {
            return; // 类型错误忽略
        }
        destList.lpush(value);

        // AOF (目标)
        storage.appendAof(new RedisArray(new RedisMessage[]{
                new BulkString("LPUSH"), new BulkString(destKey), new BulkString(value)
        }));

        // 级联唤醒
        storage.getBlockingManager().onPush(destKey, storage);
    }

    private void sendResponse(ChannelHandlerContext ctx, String key, byte[] value) {
        RedisMessage response;
        if (targetKey != null) {
            response = new BulkString(value);
        } else {
            response = new RedisArray(new RedisMessage[]{ new BulkString(key), new BulkString(value) });
        }
        ctx.writeAndFlush(response);
    }
}
