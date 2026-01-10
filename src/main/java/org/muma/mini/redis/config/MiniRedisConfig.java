package org.muma.mini.redis.config;

import lombok.Getter;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * 全局配置中心
 * 优先级: 命令行参数 > 环境变量 > 配置文件 (redis.properties) > 默认值
 */
@Getter
@Setter
public class MiniRedisConfig {

    private static final Logger log = LoggerFactory.getLogger(MiniRedisConfig.class);
    private static final MiniRedisConfig INSTANCE = new MiniRedisConfig();

    // --- Core Settings ---
    private int port = 6379;
    private int workerThreads = 0; // 0 = Netty default
    private int maxClients = 10000;

    // --- Backend Strategy ---
    private DictBackend setDictBackend = DictBackend.REDIS_DICT;

    // --- Persistence (AOF) ---
    private boolean appendOnly = false;
    private AppendFsync appendFsync = AppendFsync.EVERYSEC;
    private String appendDir = "appendonlydir";
    private String appendFilename = "appendonly.aof";
    private boolean aofUseRdbPreamble = false;

    private String configFilePath = "redis.properties"; // 默认


    // --- Replication ---
    private String slaveOfHost = null;
    private int slaveOfPort = -1;


    public static class SaveParam {
        public long seconds;
        public int changes;

        public SaveParam(long s, int c) {
            seconds = s;
            changes = c;
        }
    }

    private List<SaveParam> saveParams = new ArrayList<>();
    private String rdbFilename = "dump.rdb";


    // --- Rewrite Config ---
    // 默认 100% (即大小翻倍时重写)
    private int aofRewritePercentage = 100;
    // 默认 64MB (小于这个大小不重写)
    private long aofRewriteMinSize = 64 * 1024 * 1024;

    // --- Enums ---
    public enum AppendFsync {
        ALWAYS, EVERYSEC, NO
    }

    public enum DictBackend {
        JDK_HASHMAP, REDIS_DICT
    }

    // --- Singleton Access ---
    private MiniRedisConfig() {
    }

    public static MiniRedisConfig getInstance() {
        return INSTANCE;
    }

    // --- Loading Logic ---

    public void parseArgs(String[] args) {
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            // 【新增】支持自定义配置文件路径
            if ("--config".equals(arg) && i + 1 < args.length) {
                this.configFilePath = args[++i];
            }
            if ("--port".equals(arg) && i + 1 < args.length) {
                this.port = Integer.parseInt(args[++i]);
            } else if ("--backend".equals(arg) && i + 1 < args.length) {
                this.setDictBackend = DictBackend.valueOf(args[++i].toUpperCase());
            }
        }
        log.info("Config loaded from args: port={}, backend={}", port, setDictBackend);
    }

    public void loadConfig(String path) {
        Properties props = loadProperties(path);

        // 1. Core
        this.port = getInt(props, "server.port", this.port);
        this.workerThreads = getInt(props, "server.worker_threads", this.workerThreads);
        this.maxClients = getInt(props, "server.max_clients", this.maxClients);

        // 2. Backend
        String dictType = getString(props, "backend.set_dict", "REDIS_DICT");
        try {
            this.setDictBackend = DictBackend.valueOf(dictType.toUpperCase());
        } catch (IllegalArgumentException e) {
            log.warn("Invalid backend.set_dict value '{}', using default REDIS_DICT.", dictType);
        }

        // 3. Env Vars Override
        applyEnvOverrides();

        // 4. Persistence
        loadPersistenceConfig(props);

        // 解析 save 配置，例如 "900 1 300 10 60 10000"
        String saveStr = getString(props, "save", "900 1 300 10 60 10000");
        if (!saveStr.isEmpty()) {
            String[] parts = saveStr.split("\\s+");
            for (int i = 0; i < parts.length; i += 2) {
                saveParams.add(new SaveParam(Long.parseLong(parts[i]), Integer.parseInt(parts[i + 1])));
            }
        }

        // 解析 SlaveOf
        // 格式: slaveof <host> <port> (中间用空格分隔)
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

        log.info("MiniRedisConfig initialized: {}", this); // 需要 toString()
    }

    private Properties loadProperties(String path) {
        Properties props = new Properties();
        try (InputStream is = getClass().getClassLoader().getResourceAsStream(path)) {
            // 如果是 classpath 资源
            if (is != null) {
                props.load(is);
                log.info("Loaded config from classpath: {}", path);
            } else {
                // 尝试作为文件系统路径加载
                try (InputStream fis = new java.io.FileInputStream(path)) {
                    props.load(fis);
                    log.info("Loaded config from file: {}", path);
                } catch (IOException e) {
                    log.warn("Config file not found: {}, using defaults.", path);
                }
            }
        } catch (IOException e) {
            log.error("Error loading config", e);
        }
        return props;
    }

    private void applyEnvOverrides() {
        String envPort = System.getenv("REDIS_PORT");
        if (envPort != null) {
            this.port = Integer.parseInt(envPort);
            log.info("Port overridden by ENV: {}", this.port);
        }

        String envBackend = System.getenv("REDIS_BACKEND_SET_DICT");
        if (envBackend != null) {
            this.setDictBackend = DictBackend.valueOf(envBackend.toUpperCase());
            log.info("Backend overridden by ENV: {}", this.setDictBackend);
        }
    }

    private void loadPersistenceConfig(Properties props) {
        String aof = getString(props, "appendonly", "no");
        this.appendOnly = "yes".equalsIgnoreCase(aof);

        String fsync = getString(props, "appendfsync", "everysec");
        try {
            this.appendFsync = AppendFsync.valueOf(fsync.toUpperCase());
        } catch (IllegalArgumentException e) {
            log.warn("Invalid appendfsync value '{}', using default EVERYSEC.", fsync);
        }

        this.appendDir = getString(props, "appenddirname", "appendonlydir");
        this.appendFilename = getString(props, "appendfilename", "appendonly.aof");

        String preamble = getString(props, "aof-use-rdb-preamble", "no");
        this.aofUseRdbPreamble = "yes".equalsIgnoreCase(preamble);

        // 解析 Rewrite 配置
        String percentage = getString(props, "auto-aof-rewrite-percentage", "100");
        try {
            this.aofRewritePercentage = Integer.parseInt(percentage);
        } catch (NumberFormatException e) {
            log.warn("Invalid auto-aof-rewrite-percentage '{}', using default 100.", percentage);
        }

        String minSize = getString(props, "auto-aof-rewrite-min-size", "64mb");
        try {
            this.aofRewriteMinSize = parseSize(minSize);
        } catch (Exception e) {
            log.warn("Invalid auto-aof-rewrite-min-size '{}', using default 64MB.", minSize);
        }
    }


    // 辅助：解析带单位的大小 (64mb, 1gb)
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
        return val != null ? Integer.parseInt(val) : defaultValue;
    }

    private String getString(Properties props, String key, String defaultValue) {
        return props.getProperty(key, defaultValue);
    }

    @Override
    public String toString() {
        return "Config{port=" + port + ", aof=" + appendOnly + ", fsync=" + appendFsync + "}";
    }
}
