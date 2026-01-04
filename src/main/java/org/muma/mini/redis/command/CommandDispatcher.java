package org.muma.mini.redis.command;

import io.netty.channel.ChannelHandlerContext;
import org.muma.mini.redis.command.impl.hash.*;
import org.muma.mini.redis.command.impl.key.*;
import org.muma.mini.redis.command.impl.list.*;
import org.muma.mini.redis.command.impl.set.*;
import org.muma.mini.redis.command.impl.string.*;
import org.muma.mini.redis.command.impl.zset.*;
import org.muma.mini.redis.protocol.ErrorMessage;
import org.muma.mini.redis.protocol.RedisArray;
import org.muma.mini.redis.protocol.RedisMessage;
import org.muma.mini.redis.server.RedisContext;
import org.muma.mini.redis.store.StorageEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 *
 */
public class CommandDispatcher {

    private static final Logger log = LoggerFactory.getLogger(CommandDispatcher.class);

    private final Map<String, RedisCommand> commandMap = new HashMap<>();
    private final StorageEngine storage;

    public CommandDispatcher(StorageEngine storage) {
        this.storage = storage;
        this.initCommandRegistry();
    }

    /**
     * 初始化命令注册表，按数据结构分类注册
     */
    private void initCommandRegistry() {
        registerGenericCommands();
        registerStringCommands();
        registerHashCommands();
        registerZsetCommands();
        registerListCommands();
        registerSetCommands();


        log.info("CommandDispatcher initialized. Total commands registered: {}", commandMap.size());
    }

    private void registerSetCommands() {

        commandMap.put("SADD", new SAddCommand());
        commandMap.put("SCARD", new SCardCommand());
        commandMap.put("SISMEMBER", new SIsMemberCommand());

        commandMap.put("SMEMBERS", new SMembersCommand());
        commandMap.put("SPOP", new SPopCommand());
        commandMap.put("SRANDMEMBER", new SRandMemberCommand());
        commandMap.put("SREM", new SRemCommand());

        commandMap.put("SUNION", new SUnionCommand());
        commandMap.put("SINTER", new SInterCommand());
        commandMap.put("SDIFF", new SDiffCommand());

        commandMap.put("SMOVE", new SMoveCommand());
        commandMap.put("SSCAN", new SScanCommand());
        commandMap.put("SINTERCARD", new SInterCardCommand());
    }

    private void registerListCommands() {

        commandMap.put("LPOP", new LPopCommand());
        commandMap.put("LPUSH", new LPushCommand());
        commandMap.put("LPUSHX", new LPushXCommand());

        commandMap.put("LINDEX", new LIndexCommand());
        commandMap.put("LINSERT", new LInsertCommand());
        commandMap.put("LLEN", new LLenCommand());
        commandMap.put("LREM", new LRemCommand());
        commandMap.put("LSET", new LSetCommand());
        commandMap.put("LTRIM", new LTrimCommand());
        commandMap.put("LRANGE", new LRangeCommand());

        commandMap.put("RPOP", new RPopCommand());
        commandMap.put("RPUSH", new RPushCommand());
        commandMap.put("RPUSHX", new RPushXCommand());

        // blocking
        commandMap.put("BLPOP", new BLPopCommand());
        commandMap.put("BRPOP", new BRPopCommand());
        commandMap.put("BRPOPLPUSH", new BRPopLPushCommand());


    }

    private void registerZsetCommands() {
        commandMap.put("ZADD", new ZAddCommand());
        commandMap.put("ZRANGE", new ZRangeCommand());
        commandMap.put("ZSCORE", new ZScoreCommand());
        commandMap.put("ZCOUNT", new ZCountCommand());
        commandMap.put("ZRANGEBYSCORE", new ZRangeByScoreCommand());
        commandMap.put("ZREVRANGE", new ZRevRangeCommand());
        commandMap.put("ZSCAN", new ZScanCommand());
        commandMap.put("ZINCRBY", new ZIncrByCommand());
        commandMap.put("ZREMRANGEBYRANK", new ZRemRangeByRankCommand());
        commandMap.put("ZREMRANGEBYSCORE", new ZRemRangeByScoreCommand());
        commandMap.put("ZUNIONSTORE", new ZUnionStoreCommand());
        commandMap.put("ZINTERSTORE", new ZInterStoreCommand());


    }

    private void registerGenericCommands() {
        commandMap.put("DEL", new DelCommand());
        commandMap.put("EXPIRE", new ExpireCommand());

        // New
        commandMap.put("EXISTS", new ExistsCommand());
        commandMap.put("TTL", new TTLCommand());
        commandMap.put("PTTL", new PTTLCommand());
    }

    private void registerStringCommands() {
        commandMap.put("SET", new SetCommand());
        commandMap.put("GET", new GetCommand());
        commandMap.put("INCR", new IncrCommand());
        commandMap.put("MSET", new MSetCommand());
        commandMap.put("MGET", new MGetCommand());
        commandMap.put("SETNX", new SetNxCommand());
    }

    private void registerHashCommands() {
        commandMap.put("HSET", new HSetCommand());
        commandMap.put("HGET", new HGetCommand());
        commandMap.put("HDEL", new HDelCommand());
        commandMap.put("HGETALL", new HGetAllCommand());

        // New
        commandMap.put("HLEN", new HLenCommand());
        commandMap.put("HEXISTS", new HExistsCommand());
        commandMap.put("HINCRBY", new HIncrByCommand());
        commandMap.put("HKEYS", new HKeysCommand());
        commandMap.put("HVALS", new HValsCommand());
        commandMap.put("HMGET", new HMGetCommand());
    }

    /**
     * 核心分发逻辑
     */
    public RedisMessage dispatch(String commandName, RedisArray args, ChannelHandlerContext nettyCtx) {
        // 1. 查找命令
        String cmdUpper = commandName.toUpperCase(Locale.ROOT);
        RedisCommand command = commandMap.get(cmdUpper);

        if (command == null) {
            log.warn("Command not found: {}", commandName);
            return new ErrorMessage("ERR unknown command '" + commandName + "'");
        }

        // 2. 执行并监控耗时
        long startTime = System.nanoTime();
        try {
            // 封装 Context
            RedisContext context = new RedisContext(nettyCtx);
            RedisMessage response = command.execute(storage, args, context);

            // 记录慢日志 (比如超过 10ms)
            long duration = (System.nanoTime() - startTime) / 1000_000; // ms
            if (duration > 10) {
                log.warn("Slow command detected: {} cost {}ms", commandName, duration);
            } else if (log.isDebugEnabled()) {
                log.debug("Command executed: {} cost {}ms", commandName, duration);
            }

            return response;

        } catch (IllegalArgumentException | IllegalStateException e) {
            // 预期内的业务错误 (如参数错误、类型转换错误)
            log.warn("Command execution failed (Client Error): {} - {}", commandName, e.getMessage());
            return new ErrorMessage("ERR " + e.getMessage());

        } catch (Exception e) {
            // 意料之外的系统错误 (如 NPE, IO Error)
            log.error("Internal Server Error processing command: {}", commandName, e);
            return new ErrorMessage("ERR internal server error");
        }
    }
}
