package org.muma.mini.redis.config;

import lombok.Getter;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
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

    // --- Enums ---
    public enum AppendFsync {
        ALWAYS, EVERYSEC, NO
    }

    public enum DictBackend {
        JDK_HASHMAP, REDIS_DICT
    }

    // --- Singleton Access ---
    private MiniRedisConfig() {
        loadConfig();
    }

    public static MiniRedisConfig getInstance() {
        return INSTANCE;
    }

    // --- Loading Logic ---

    public void parseArgs(String[] args) {
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            if ("--port".equals(arg) && i + 1 < args.length) {
                this.port = Integer.parseInt(args[++i]);
            } else if ("--backend".equals(arg) && i + 1 < args.length) {
                this.setDictBackend = DictBackend.valueOf(args[++i].toUpperCase());
            }
        }
        log.info("Config loaded from args: port={}, backend={}", port, setDictBackend);
    }

    private void loadConfig() {
        Properties props = loadProperties();

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

        log.info("MiniRedisConfig initialized: {}", this); // 需要 toString()
    }

    private Properties loadProperties() {
        Properties props = new Properties();
        try (InputStream is = getClass().getClassLoader().getResourceAsStream("redis.properties")) {
            if (is != null) {
                props.load(is);
            } else {
                log.warn("redis.properties not found in classpath, using defaults.");
            }
        } catch (IOException e) {
            log.error("Failed to load redis.properties", e);
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
