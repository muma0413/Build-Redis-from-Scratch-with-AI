package org.muma.mini.redis.command.impl.key;

import org.muma.mini.redis.command.RedisCommand;
import org.muma.mini.redis.common.RedisData;
import org.muma.mini.redis.protocol.*;
import org.muma.mini.redis.server.RedisContext;
import org.muma.mini.redis.store.StorageEngine;

import java.util.Locale;

/**
 * EXPIRE key seconds [NX|XX|GT|LT]
 */
public class ExpireCommand implements RedisCommand {
    @Override
    public RedisMessage execute(StorageEngine storage, RedisArray args, RedisContext context) {
        RedisMessage[] elements = args.elements();
        if (elements.length < 3) return errorArgs("expire");

        String key = ((BulkString) elements[1]).asString();
        long seconds;
        try {
            String secStr = ((BulkString) elements[2]).asString();
            if (secStr == null) return errorArgs("expire");
            seconds = Long.parseLong(secStr);
        } catch (NumberFormatException e) {
            return errorInt();
        }

        // 解析选项
        boolean nx = false, xx = false, gt = false, lt = false;
        if (elements.length > 3) {
            String opt = ((BulkString) elements[3]).asString().toUpperCase(Locale.ROOT);
            switch (opt) {
                case "NX" -> nx = true;
                case "XX" -> xx = true;
                case "GT" -> gt = true;
                case "LT" -> lt = true;
                default -> {
                    return new ErrorMessage("ERR unsupported option");
                }
            }
        }

        synchronized (storage.getLock(key)) {
            RedisData<?> data = storage.get(key);
            if (data == null) {
                return new RedisInteger(0);
            }

            long currentExpire = data.getExpireAt();
            long newExpire = System.currentTimeMillis() + (seconds * 1000);

            // 检查条件
            if (nx && currentExpire != -1) return new RedisInteger(0); // 已有过期，NX 失败
            if (xx && currentExpire == -1) return new RedisInteger(0); // 无过期，XX 失败
            if (gt && (currentExpire == -1 || newExpire <= currentExpire)) return new RedisInteger(0); // 不大于，GT 失败
            if (lt && currentExpire != -1 && newExpire >= currentExpire) return new RedisInteger(0); // 不小于，LT 失败

            data.setExpireAt(newExpire);
            storage.put(key, data);
        }

        return new RedisInteger(1);
    }
}
