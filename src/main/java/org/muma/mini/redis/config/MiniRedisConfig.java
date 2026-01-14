package org.muma.mini.redis.config;

import lombok.Getter;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Mini-Redis 全局配置中心 (Singleton)
 * <p>
 * 负责管理服务器所有的运行时参数。
 * <p>
 * 【配置加载优先级】(从高到低)
 * 1. 命令行参数 (如 --port 6380)
 * 2. 环境变量 (如 REDIS_PORT=6380)
 * 3. 配置文件 (redis.properties 或通过 --config 指定的文件)
 * 4. 硬编码默认值
 */
@Getter
@Setter
public class MiniRedisConfig {

    private static final Logger log = LoggerFactory.getLogger(MiniRedisConfig.class);

    // 单例实例
    private static final MiniRedisConfig INSTANCE = new MiniRedisConfig();

    // ==========================================
    // 核心配置 (Core Settings)
    // ==========================================

    // 服务监听端口
    private int port = 6379;

    // Netty Worker 线程数 (0 表示使用 Netty 默认值: CPU核数 * 2)
    private int workerThreads = 0;

    // 最大客户端连接数
    private int maxClients = 10000;

    // ==========================================
    // 存储后端策略 (Backend Strategy)
    // ==========================================

    // Set/Hash 底层使用 JDK HashMap 还是 自研 RedisDict
    private DictBackend setDictBackend = DictBackend.REDIS_DICT;

    public enum DictBackend {
        JDK_HASHMAP, // 高吞吐，但在扩容时会有 STW 风险
        REDIS_DICT   // 渐进式 Rehash，低延迟，推荐生产使用
    }

    // ==========================================
    // 持久化配置 (Persistence)
    // ==========================================

    // --- AOF (Append Only File) ---
    private boolean appendOnly = false; // 默认关闭
    private AppendFsync appendFsync = AppendFsync.EVERYSEC; // 刷盘策略
    private String appendDir = "appendonlydir"; // AOF 文件目录
    private String appendFilename = "appendonly.aof"; // AOF 主文件名
    private boolean aofUseRdbPreamble = false; // 混合持久化 (暂未实现)

    // AOF Rewrite 触发条件
    private int aofRewritePercentage = 100; // 增长百分比 (100%)
    private long aofRewriteMinSize = 64 * 1024 * 1024; // 最小重写体积 (64MB)

    public enum AppendFsync {
        ALWAYS,   // 每次写入都刷盘 (慢，最安全)
        EVERYSEC, // 每秒刷盘 (默认，折中)
        NO        // 操作系统自己决定 (快，不安全)
    }

    // --- RDB (Snapshot) ---
    private String dir = "."; // 默认当前目录
    private String rdbFilename = "dump.rdb";
    // 自动保存策略列表 (seconds, changes)
    private List<SaveParam> saveParams = new ArrayList<>();

    public static class SaveParam {
        public long seconds;
        public int changes;

        public SaveParam(long s, int c) {
            seconds = s;
            changes = c;
        }
    }

    // ==========================================
    // 主从复制 (Replication)
    // ==========================================

    private String slaveOfHost = null; // Master IP
    private int slaveOfPort = -1;      // Master Port

    // ==========================================
    // 初始化逻辑
    // ==========================================

    /**
     * 私有构造函数。
     * 注意：这里不自动调用 loadConfig，因为需要等待 main 方法解析 --config 参数。
     */
    private MiniRedisConfig() {
    }

    public static MiniRedisConfig getInstance() {
        return INSTANCE;
    }

    /**
     * 加载配置的核心入口
     *
     * @param path 配置文件路径 (classpath 相对路径 或 文件系统绝对路径)
     */
    public void loadConfig(String path) {
        Properties props = loadProperties(path);

        // 1. 解析基础配置
        this.port = getInt(props, "server.port", this.port);
        this.workerThreads = getInt(props, "server.worker_threads", this.workerThreads);
        this.maxClients = getInt(props, "server.max_clients", this.maxClients);

        // 2. 解析后端策略
        String dictType = getString(props, "backend.set_dict", "REDIS_DICT");
        try {
            this.setDictBackend = DictBackend.valueOf(dictType.toUpperCase());
        } catch (IllegalArgumentException e) {
            log.warn("Invalid backend.set_dict value '{}', using default REDIS_DICT.", dictType);
        }

        // 3. 解析主从配置 (slaveof host port)
        String slaveof = getString(props, "slaveof", "");
        if (!slaveof.isEmpty()) {
            String[] parts = slaveof.split("\\s+");
            if (parts.length == 2) {
                this.slaveOfHost = parts[0];
                try {
                    this.slaveOfPort = Integer.parseInt(parts[1]);
                } catch (NumberFormatException e) {
                    log.warn("Invalid slaveof port: {}", parts[1]);
                }
            } else {
                log.warn("Invalid slaveof config format: {}", slaveof);
            }
        }

        // 4. 应用环境变量覆盖 (Docker 友好)
        applyEnvOverrides();

        // 5. 加载持久化配置
        loadPersistenceConfig(props);

        // 6. 解析 RDB Save 规则
        // 格式: "900 1 300 10 60 10000"
        String saveStr = getString(props, "save", "900 1 300 10 60 10000");
        if (!saveStr.isEmpty()) {
            saveParams.clear(); // 清空默认值
            String[] parts = saveStr.split("\\s+");
            // 成对解析
            for (int i = 0; i < parts.length; i += 2) {
                if (i + 1 < parts.length) {
                    try {
                        long sec = Long.parseLong(parts[i]);
                        int changes = Integer.parseInt(parts[i + 1]);
                        saveParams.add(new SaveParam(sec, changes));
                    } catch (NumberFormatException e) {
                        log.warn("Invalid save param: {} {}", parts[i], parts[i + 1]);
                    }
                }
            }
        }

        log.info("MiniRedisConfig initialized: {}", this);
    }

    /**
     * 解析命令行参数，覆盖现有配置
     *
     * @param args main 方法传入的参数
     */
    public void parseArgs(String[] args) {
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];

            // --config 在 main 方法中已经处理过用于加载文件，这里忽略
            if ("--config".equals(arg) && i + 1 < args.length) {
                i++;
                continue;
            }

            // --port 6380
            if ("--port".equals(arg) && i + 1 < args.length) {
                this.port = Integer.parseInt(args[++i]);
            }
            // --backend JDK_HASHMAP
            else if ("--backend".equals(arg) && i + 1 < args.length) {
                try {
                    this.setDictBackend = DictBackend.valueOf(args[++i].toUpperCase());
                } catch (IllegalArgumentException e) {
                    log.warn("Invalid backend arg: {}", args[i]);
                }
            }
        }
        log.info("Config overrides applied from command line args.");
    }

    // --- 内部辅助方法 ---

    private Properties loadProperties(String path) {
        Properties props = new Properties();
        // 优先从 Classpath 加载 (jar 包内)
        try (InputStream is = getClass().getClassLoader().getResourceAsStream(path)) {
            if (is != null) {
                props.load(is);
                log.info("Loaded config from classpath: {}", path);
                return props;
            }
        } catch (IOException e) {
            log.error("Failed to load config from classpath", e);
        }

        // 其次尝试从文件系统加载 (外部挂载)
        try (InputStream fis = new FileInputStream(path)) {
            props.load(fis);
            log.info("Loaded config from file system: {}", path);
        } catch (IOException e) {
            log.warn("Config file '{}' not found, using internal defaults.", path);
        }
        return props;
    }

    private void applyEnvOverrides() {
        String envPort = System.getenv("REDIS_PORT");
        if (envPort != null) {
            try {
                this.port = Integer.parseInt(envPort);
                log.info("Port overridden by ENV: {}", this.port);
            } catch (NumberFormatException ignored) {
            }
        }

        String envBackend = System.getenv("REDIS_BACKEND_SET_DICT");
        if (envBackend != null) {
            try {
                this.setDictBackend = DictBackend.valueOf(envBackend.toUpperCase());
                log.info("Backend overridden by ENV: {}", this.setDictBackend);
            } catch (IllegalArgumentException ignored) {
            }
        }
    }

    // 新增字段 (记得在类头部定义)
    // private String dir = ".";
    // private String dbFilename = "dump.rdb";

    private void loadPersistenceConfig(Properties props) {
        // 1. 基础工作目录 (Working Directory)
        this.dir = getString(props, "dir", ".");

        // 2. RDB 文件名
        this.rdbFilename = getString(props, "dbfilename", "dump.rdb");

        // 3. AOF 基础配置
        String aof = getString(props, "appendonly", "no");
        this.appendOnly = "yes".equalsIgnoreCase(aof);

        String fsync = getString(props, "appendfsync", "everysec");
        try {
            this.appendFsync = AppendFsync.valueOf(fsync.toUpperCase());
        } catch (IllegalArgumentException e) {
            log.warn("Invalid appendfsync value '{}', using default EVERYSEC.", fsync);
        }

        // AOF 目录和文件名
        // 注意：Redis 7.0 规范中 appenddirname 是 dir 下的子目录名
        this.appendDir = getString(props, "appenddirname", "appendonlydir");
        this.appendFilename = getString(props, "appendfilename", "appendonly.aof");

        String preamble = getString(props, "aof-use-rdb-preamble", "no");
        this.aofUseRdbPreamble = "yes".equalsIgnoreCase(preamble);

        // 4. AOF Rewrite 配置
        String percentage = getString(props, "auto-aof-rewrite-percentage", "100");
        try {
            this.aofRewritePercentage = Integer.parseInt(percentage);
        } catch (NumberFormatException e) {
            log.warn("Invalid rewrite percentage: {}", percentage);
        }

        String minSize = getString(props, "auto-aof-rewrite-min-size", "64mb");
        try {
            this.aofRewriteMinSize = parseSize(minSize);
        } catch (Exception e) {
            log.warn("Invalid rewrite min size: {}", minSize);
        }
    }

    // 辅助方法：获取 RDB 完整文件对象
    public File getRdbFile() {
        return new File(dir, rdbFilename);
    }

    // 辅助方法：获取 AOF 完整目录对象
    public File getAofDirFile() {
        return new File(dir, appendDir);
    }

    public File getWorkingDir() {
        return new File(dir);
    }


    private long parseSize(String sizeStr) {
        if (sizeStr == null) return 64 * 1024 * 1024;
        String s = sizeStr.toLowerCase().trim();
        long multiplier = 1;
        if (s.endsWith("kb")) {
            multiplier = 1024;
            s = s.substring(0, s.length() - 2);
        } else if (s.endsWith("mb")) {
            multiplier = 1024 * 1024;
            s = s.substring(0, s.length() - 2);
        } else if (s.endsWith("gb")) {
            multiplier = 1024 * 1024 * 1024;
            s = s.substring(0, s.length() - 2);
        } else if (s.endsWith("b")) {
            s = s.substring(0, s.length() - 1);
        }
        return Long.parseLong(s.trim()) * multiplier;
    }

    private int getInt(Properties props, String key, int defaultValue) {
        String val = props.getProperty(key);
        return val != null ? Integer.parseInt(val.trim()) : defaultValue;
    }

    private String getString(Properties props, String key, String defaultValue) {
        return props.getProperty(key, defaultValue).trim();
    }

    @Override
    public String toString() {
        return "MiniRedisConfig{" +
                "port=" + port +
                ", setDictBackend=" + setDictBackend +
                ", appendOnly=" + appendOnly +
                ", slaveOf=" + (slaveOfHost != null ? slaveOfHost + ":" + slaveOfPort : "none") +
                '}';
    }
}
