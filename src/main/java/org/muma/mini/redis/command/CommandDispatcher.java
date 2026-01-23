package org.muma.mini.redis.command;

import io.netty.channel.ChannelHandlerContext;
import org.muma.mini.redis.aof.AofManager;
import org.muma.mini.redis.command.impl.bf.BfAddCommand;
import org.muma.mini.redis.command.impl.bf.BfExistsCommand;
import org.muma.mini.redis.command.impl.bf.BfReserveCommand;
import org.muma.mini.redis.command.impl.hash.*;
import org.muma.mini.redis.command.impl.hll.PfAddCommand;
import org.muma.mini.redis.command.impl.hll.PfCountCommand;
import org.muma.mini.redis.command.impl.hll.PfMergeCommand;
import org.muma.mini.redis.command.impl.key.*;
import org.muma.mini.redis.command.impl.list.*;
import org.muma.mini.redis.command.impl.replication.DebugBreakCommand;
import org.muma.mini.redis.command.impl.replication.PsyncCommand;
import org.muma.mini.redis.command.impl.replication.ReplConfCommand;
import org.muma.mini.redis.command.impl.server.PingCommand;
import org.muma.mini.redis.command.impl.server.SlaveOfCommand;
import org.muma.mini.redis.command.impl.set.*;
import org.muma.mini.redis.command.impl.string.*;
import org.muma.mini.redis.command.impl.zset.*;
import org.muma.mini.redis.config.MiniRedisConfig;
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
 * 命令分发器 (Dispatcher)
 * <p>
 * 核心职责：
 * 1. 维护命令注册表 (Registry)，将字符串命令映射到 Command 实例。
 * 2. 接收 Netty 解析后的 RedisArray，分发给具体 Command 执行。
 * 3. 管理执行上下文 (Context) 和异常处理。
 * 4. 触发 AOF 持久化和主从复制传播 (Propagation)。
 * <p>
 * 线程模型：
 * 该类是无状态的单例（依赖注入了 Storage 等组件），方法由 RedisCoreExecutor 单线程调用，
 * 因此内部逻辑不需要加锁，天然线程安全。
 */
public class CommandDispatcher {

    private static final Logger log = LoggerFactory.getLogger(CommandDispatcher.class);

    private final Map<String, RedisCommand> commandMap = new HashMap<>();

    // Core Dependencies
    private final StorageEngine storage;
    private final AofManager aofManager;
    private final ReplicationManager replicationManager;
    private final RdbManager rdbManager;

    // Config Cache (Hot Path Optimization)
    private final boolean appendOnlyEnabled;

    public CommandDispatcher(StorageEngine storage,
                             AofManager aofManager,
                             ReplicationManager replManager,
                             RdbManager rdbManager,
                             MiniRedisConfig config) {
        this.storage = storage;
        this.aofManager = aofManager;
        this.replicationManager = replManager;
        this.rdbManager = rdbManager;
        this.appendOnlyEnabled = config.isAppendOnly();

        initCommandRegistry();
    }

    /**
     * 初始化命令注册表，按功能模块分类注册。
     * 开闭原则：新增命令只需修改此处注册逻辑。
     */
    private void initCommandRegistry() {
        registerGenericCommands();
        registerStringCommands();
        registerHashCommands();
        registerListCommands();
        registerSetCommands();
        registerZSetCommands();
        registerBloomCommands();
        registerReplicationCommands();
        registerHllCommands();

        log.info("CommandDispatcher initialized. Total commands registered: {}", commandMap.size());
    }

    // --- Registry Helpers ---

    private void registerGenericCommands() {
        commandMap.put("DEL", new DelCommand());
        commandMap.put("EXPIRE", new ExpireCommand());
        commandMap.put("EXISTS", new ExistsCommand());
        commandMap.put("TTL", new TTLCommand());
        commandMap.put("PTTL", new PTTLCommand());
    }


    private void registerHllCommands() {
        commandMap.put("PFADD", new PfAddCommand());
        commandMap.put("PFCOUNT", new PfCountCommand());
        commandMap.put("PFMERGE", new PfMergeCommand());
    }

    private void registerStringCommands() {
        // Basic
        commandMap.put("SET", new SetCommand());
        commandMap.put("GET", new GetCommand());
        commandMap.put("MSET", new MSetCommand());
        commandMap.put("MGET", new MGetCommand());
        commandMap.put("SETNX", new SetNxCommand());
        commandMap.put("GETEX", new GetExCommand());
        commandMap.put("APPEND", new AppendCommand());
        commandMap.put("STRLEN", new StrLenCommand());

        // Counter
        commandMap.put("INCR", new IncrCommand());
        commandMap.put("INCRBY", new IncrByCommand());
        commandMap.put("DECR", new DecrCommand());
        commandMap.put("DECRBY", new DecrByCommand());

        // Bitmap
        commandMap.put("SETBIT", new SetBitCommand());
        commandMap.put("GETBIT", new GetBitCommand());
        commandMap.put("BITCOUNT", new BitCountCommand());
        commandMap.put("BITOP", new BitOpCommand());
        commandMap.put("BITPOS", new BitPosCommand());
    }

    private void registerHashCommands() {
        commandMap.put("HSET", new HSetCommand());
        commandMap.put("HGET", new HGetCommand());
        commandMap.put("HMGET", new HMGetCommand());
        commandMap.put("HDEL", new HDelCommand());
        commandMap.put("HLEN", new HLenCommand());
        commandMap.put("HEXISTS", new HExistsCommand());
        commandMap.put("HINCRBY", new HIncrByCommand());
        commandMap.put("HKEYS", new HKeysCommand());
        commandMap.put("HVALS", new HValsCommand());
        commandMap.put("HGETALL", new HGetAllCommand());
        commandMap.put("HSCAN", new HScanCommand());
        commandMap.put("HINCRBYFLOAT", new HIncrByFloatCommand());
    }

    private void registerListCommands() {
        // Push / Pop
        commandMap.put("LPUSH", new LPushCommand());
        commandMap.put("LPUSHX", new LPushXCommand());
        commandMap.put("LPOP", new LPopCommand());
        commandMap.put("RPUSH", new RPushCommand());
        commandMap.put("RPUSHX", new RPushXCommand());
        commandMap.put("RPOP", new RPopCommand());

        // Blocking
        commandMap.put("BLPOP", new BLPopCommand());
        commandMap.put("BRPOP", new BRPopCommand());
        commandMap.put("BRPOPLPUSH", new BRPopLPushCommand());

        // Ops
        commandMap.put("LLEN", new LLenCommand());
        commandMap.put("LINDEX", new LIndexCommand());
        commandMap.put("LSET", new LSetCommand());
        commandMap.put("LINSERT", new LInsertCommand());
        commandMap.put("LREM", new LRemCommand());
        commandMap.put("LTRIM", new LTrimCommand());
        commandMap.put("LRANGE", new LRangeCommand());
    }

    private void registerSetCommands() {
        commandMap.put("SADD", new SAddCommand());
        commandMap.put("SREM", new SRemCommand());
        commandMap.put("SISMEMBER", new SIsMemberCommand());
        commandMap.put("SCARD", new SCardCommand());
        commandMap.put("SMEMBERS", new SMembersCommand());
        commandMap.put("SPOP", new SPopCommand());
        commandMap.put("SRANDMEMBER", new SRandMemberCommand());
        commandMap.put("SMOVE", new SMoveCommand());
        commandMap.put("SSCAN", new SScanCommand());

        // Operations
        commandMap.put("SUNION", new SUnionCommand());
        commandMap.put("SINTER", new SInterCommand());
        commandMap.put("SDIFF", new SDiffCommand());
        commandMap.put("SINTERCARD", new SInterCardCommand());
    }

    private void registerZSetCommands() {
        commandMap.put("ZADD", new ZAddCommand());
        commandMap.put("ZSCORE", new ZScoreCommand());
        commandMap.put("ZINCRBY", new ZIncrByCommand());
        commandMap.put("ZCARD", new ZCountCommand()); // Alias or implementation
        commandMap.put("ZCOUNT", new ZCountCommand());

        // Range
        commandMap.put("ZRANGE", new ZRangeCommand());
        commandMap.put("ZREVRANGE", new ZRevRangeCommand());
        commandMap.put("ZRANGEBYSCORE", new ZRangeByScoreCommand());
        commandMap.put("ZSCAN", new ZScanCommand());
        commandMap.put("ZRANK", new ZRankCommand());
        commandMap.put("ZREVRANK", new ZRevRankCommand());

        // Rem Range
        commandMap.put("ZREMRANGEBYRANK", new ZRemRangeByRankCommand());
        commandMap.put("ZREMRANGEBYSCORE", new ZRemRangeByScoreCommand());

        // Ops
        commandMap.put("ZUNIONSTORE", new ZUnionStoreCommand());
        commandMap.put("ZINTERSTORE", new ZInterStoreCommand());
    }

    private void registerBloomCommands() {
        commandMap.put("BF.RESERVE", new BfReserveCommand());
        commandMap.put("BF.ADD", new BfAddCommand());
        commandMap.put("BF.EXISTS", new BfExistsCommand());
    }

    private void registerReplicationCommands() {
        commandMap.put("SLAVEOF", new SlaveOfCommand(replicationManager));
        commandMap.put("REPLCONF", new ReplConfCommand());
        commandMap.put("PSYNC", new PsyncCommand(replicationManager, rdbManager));
        commandMap.put("PING", new PingCommand());
        commandMap.put("DEBUG", new DebugBreakCommand(replicationManager));
    }

    // --- Core Dispatch Logic ---

    /**
     * 重载方法：用于 AOF 重放 (无网络上下文)
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
     * 负责 Command 查找、Context 封装、执行、耗时统计以及 AOF/Replication 钩子触发。
     */
    public RedisMessage dispatch(String commandName, RedisArray args, ChannelHandlerContext nettyCtx) {
        // 1. 查找命令 (Fast Lookup)
        String cmdUpper = commandName.toUpperCase(Locale.ROOT);
        RedisCommand command = commandMap.get(cmdUpper);

        if (command == null) {
            log.warn("Command not found: {}", commandName);
            return new ErrorMessage("ERR unknown command '" + commandName + "'");
        }

        // 2. 执行并监控耗时
        long startTime = System.nanoTime();
        try {
            RedisContext context = new RedisContext(nettyCtx);
            RedisMessage response = command.execute(storage, args, context);

            // 记录慢日志 (> 10ms)
            long duration = (System.nanoTime() - startTime) / 1_000_000;
            if (duration > 10) {
                log.warn("Slow command detected: {} cost {}ms", commandName, duration);
            } else if (log.isDebugEnabled()) {
                log.debug("Command executed: {} cost {}ms", commandName, duration);
            }

            // 3. 后置处理：持久化与复制 (Write-Behind)
            // 只有当命令是写操作且执行成功时才触发
            if (command.isWrite() && !(response instanceof ErrorMessage)) {
                handleBackgroundTasks(commandName, args);
            }

            return response;

        } catch (IllegalArgumentException | IllegalStateException e) {
            // 预期内的业务错误 (Client Side Error)
            log.warn("Command execution failed (Client Error): {} - {}", commandName, e.getMessage());
            return new ErrorMessage("ERR " + e.getMessage());

        } catch (Exception e) {
            // 意料之外的系统错误 (Server Side Error)
            log.error("Internal Server Error processing command: {}", commandName, e);
            return new ErrorMessage("ERR internal server error");
        }
    }

    /**
     * 处理后台任务 (AOF + Replication)
     * 使用 try-catch 包裹，确保核心业务不因副流程失败而中断。
     */
    private void handleBackgroundTasks(String commandName, RedisArray args) {
        try {
            // 1. AOF 追加
            if (appendOnlyEnabled) {
                aofManager.append(args);
            }

            // 2. Replication 传播
            // ReplicationManager 内部会判断当前角色，如果是 Slave 则不传播
            replicationManager.propagate(args);

        } catch (Exception e) {
            log.error("Failed to process background tasks (AOF/Repl) for command: {}", commandName, e);
        }
    }
}
