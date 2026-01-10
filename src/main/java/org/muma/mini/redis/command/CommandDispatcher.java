package org.muma.mini.redis.command;

import io.netty.channel.ChannelHandlerContext;
import org.muma.mini.redis.aof.AofManager;
import org.muma.mini.redis.command.impl.bf.BfAddCommand;
import org.muma.mini.redis.command.impl.bf.BfExistsCommand;
import org.muma.mini.redis.command.impl.bf.BfReserveCommand;
import org.muma.mini.redis.command.impl.hash.*;
import org.muma.mini.redis.command.impl.key.*;
import org.muma.mini.redis.command.impl.list.*;
import org.muma.mini.redis.command.impl.replication.PsyncCommand;
import org.muma.mini.redis.command.impl.replication.ReplConfCommand;
import org.muma.mini.redis.command.impl.server.SlaveOfCommand;
import org.muma.mini.redis.command.impl.set.*;
import org.muma.mini.redis.command.impl.string.*;
import org.muma.mini.redis.command.impl.zset.*;
import org.muma.mini.redis.protocol.BulkString;
import org.muma.mini.redis.protocol.ErrorMessage;
import org.muma.mini.redis.protocol.RedisArray;
import org.muma.mini.redis.protocol.RedisMessage;
import org.muma.mini.redis.rdb.RdbManager;
import org.muma.mini.redis.replication.ReplicationManager;
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
    private final AofManager aofManager; // 【新增】


    private final ReplicationManager replicationManager;
    private final RdbManager rdbManager;

    public CommandDispatcher(StorageEngine storage, AofManager aofManager,
                             ReplicationManager replManager, RdbManager rdbManager) {
        this.storage = storage;
        this.aofManager = aofManager;
        this.replicationManager = replManager;
        this.rdbManager = rdbManager;
        initCommandRegistry();
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
        registerBloomCommands();
        registerReplicationCommands();

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

    private void registerBloomCommands() {
        commandMap.put("BF.RESERVE", new BfReserveCommand());
        commandMap.put("BF.ADD", new BfAddCommand());
        commandMap.put("BF.EXISTS", new BfExistsCommand());
    }

    // 需要注入 ReplicationManager 和 RdbManager
    // 构造函数可能需要调整

    private void registerReplicationCommands() {
        commandMap.put("SLAVEOF", new SlaveOfCommand(replicationManager));
        commandMap.put("REPLCONF", new ReplConfCommand());
        commandMap.put("PSYNC", new PsyncCommand(replicationManager, rdbManager));
    }

    private void registerStringCommands() {
        commandMap.put("SET", new SetCommand());
        commandMap.put("GETEX", new GetExCommand());
        commandMap.put("GET", new GetCommand());
        commandMap.put("INCR", new IncrCommand());
        commandMap.put("MSET", new MSetCommand());
        commandMap.put("MGET", new MGetCommand());
        commandMap.put("SETNX", new SetNxCommand());
        commandMap.put("DECRBY", new DecrByCommand());
        commandMap.put("DECR", new DecrCommand());
        commandMap.put("INCRBY", new IncrByCommand());
        commandMap.put("GETBIT", new GetBitCommand());
        commandMap.put("SETBIT", new SetBitCommand());
        commandMap.put("BITCOUNT", new BitCountCommand());
        commandMap.put("APPEND", new AppendCommand());
        commandMap.put("STRLEN", new StrLenCommand());

        commandMap.put("BITOP", new BitOpCommand());
        commandMap.put("BITPOS", new BitPosCommand());
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
     * 重载方法：用于 AOF 重放
     * 从 RedisArray 中提取命令名并分发
     */
    public RedisMessage dispatch(RedisArray args, ChannelHandlerContext ctx) {
        RedisMessage[] elements = args.elements();
        if (elements == null || elements.length == 0) return null;

        if (!(elements[0] instanceof BulkString cmdNameBulk)) {
            return new ErrorMessage("ERR protocol error: command name must be string");
        }

        String commandName = cmdNameBulk.asString().toUpperCase(Locale.ROOT);
        return dispatch(commandName, args, ctx);
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

            // 【核心 AOF 逻辑】
            // 1. 命令标记为写操作
            // 2. 执行没有报错 (不是 ErrorMessage)
            // 3. AOF 开启中 (Manager 内部会判断)
            // 4. 注意：这里 args 已经是 RedisArray，可以直接存
            if (command.isWrite() && !(response instanceof ErrorMessage)) {
                aofManager.append(args);
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
