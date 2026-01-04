package org.muma.mini.redis.command.impl.string;


import org.muma.mini.redis.command.RedisCommand;
import org.muma.mini.redis.common.RedisData;
import org.muma.mini.redis.common.RedisDataType;
import org.muma.mini.redis.protocol.BulkString;
import org.muma.mini.redis.protocol.ErrorMessage;
import org.muma.mini.redis.protocol.RedisArray;
import org.muma.mini.redis.protocol.RedisMessage;
import org.muma.mini.redis.server.RedisContext;
import org.muma.mini.redis.store.StorageEngine;

public class GetCommand implements RedisCommand {
    @Override
    public RedisMessage execute(StorageEngine storage,RedisArray args, RedisContext context) {
        if (args.elements().length != 2) {
            return new ErrorMessage("ERR wrong number of arguments for 'get' command");
        }

        String key = ((BulkString) args.elements()[1]).asString();
        RedisData<?> data = storage.get(key);

        if (data == null) {
            return new BulkString((byte[]) null); // Nil
        }

        if (data.getType() != RedisDataType.STRING) {
            return new ErrorMessage("WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        return new BulkString((byte[]) data.getData());
    }
}
