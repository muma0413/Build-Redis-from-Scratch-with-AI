package org.muma.mini.redis.config;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * 全局配置中心
 * 优先级: 命令行参数 > 环境变量 > 配置文件 (redis.properties) > 默认值
 */
public class MiniRedisConfig {

    private static final MiniRedisConfig INSTANCE = new MiniRedisConfig();

    // --- 配置项定义 ---
    private int port = 6379;
    private int workerThreads = 0; // 0 = Netty default (CPU * 2)
    private DictBackend setDictBackend = DictBackend.REDIS_DICT;
    private int maxClients = 10000;

    // 简单的枚举定义
    public enum DictBackend {
        JDK_HASHMAP, REDIS_DICT
    }

    private MiniRedisConfig() {
        // 私有构造，防止外部实例化
        loadConfig();
    }

    public static MiniRedisConfig getInstance() {
        return INSTANCE;
    }

    // --- 加载逻辑 ---

    private void loadConfig() {
        // 1. 加载 classpath 下的 redis.properties
        Properties props = new Properties();
        try (InputStream is = getClass().getClassLoader().getResourceAsStream("redis.properties")) {
            if (is != null) {
                props.load(is);
            }
        } catch (IOException e) {
            System.err.println("Warning: Could not load redis.properties, using defaults.");
        }

        // 2. 解析配置 (Properties -> Fields)
        this.port = getInt(props, "server.port", this.port);
        this.workerThreads = getInt(props, "server.worker_threads", this.workerThreads);
        this.maxClients = getInt(props, "server.max_clients", this.maxClients);

        String dictType = getString(props, "backend.set_dict", "REDIS_DICT");
        try {
            this.setDictBackend = DictBackend.valueOf(dictType.toUpperCase());
        } catch (IllegalArgumentException e) {
            System.err.println("Warning: Invalid backend.set_dict value, using default REDIS_DICT");
        }

        // 3. 环境变量覆盖 (Env Vars)
        // 约定：REDIS_PORT, REDIS_BACKEND_SET_DICT
        String envPort = System.getenv("REDIS_PORT");
        if (envPort != null) this.port = Integer.parseInt(envPort);

        String envBackend = System.getenv("REDIS_BACKEND_SET_DICT");
        if (envBackend != null) this.setDictBackend = DictBackend.valueOf(envBackend.toUpperCase());
    }

    // 暴露给 main 方法解析命令行参数
    // --port 6380 --backend JDK_HASHMAP
    public void parseArgs(String[] args) {
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            if ("--port".equals(arg) && i + 1 < args.length) {
                this.port = Integer.parseInt(args[++i]);
            } else if ("--backend".equals(arg) && i + 1 < args.length) {
                this.setDictBackend = DictBackend.valueOf(args[++i].toUpperCase());
            }
        }
    }

    // --- 辅助工具 ---

    private int getInt(Properties props, String key, int defaultValue) {
        String val = props.getProperty(key);
        return val != null ? Integer.parseInt(val) : defaultValue;
    }

    private String getString(Properties props, String key, String defaultValue) {
        return props.getProperty(key, defaultValue);
    }

    // --- Getters (只读，防止运行时被随意修改) ---

    public int getPort() {
        return port;
    }

    public int getWorkerThreads() {
        return workerThreads;
    }

    public DictBackend getSetDictBackend() {
        return setDictBackend;
    }

    public int getMaxClients() {
        return maxClients;
    }

    // Setters 仅保留必要的，或者干脆移除，保持 Config 不可变
    // 如果需要动态修改 (如 CONFIG SET 命令)，再加回去。
    public void setSetDictBackend(DictBackend backend) {
        this.setDictBackend = backend;
    }
}
