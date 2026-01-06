package org.muma.mini.redis.aof;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.muma.mini.redis.config.MiniRedisConfig;
import org.muma.mini.redis.protocol.BulkString;
import org.muma.mini.redis.protocol.RedisArray;
import org.muma.mini.redis.protocol.RedisMessage;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AofManagerTest {

    private static final String TEST_DIR = "target/aof_manager_test";
    private MiniRedisConfig config;
    private AofManager manager;

    @BeforeEach
    void setUp() throws IOException {
        // 清理环境
        deleteDir(new File(TEST_DIR));

        config = MiniRedisConfig.getInstance();
        config.setAppendDir(TEST_DIR);
        config.setAppendOnly(true); // 必须开启
        config.setAppendFsync(MiniRedisConfig.AppendFsync.ALWAYS); // 方便测试立即刷盘

        manager = new AofManager(config);
    }

    @AfterEach
    void tearDown() {
        if (manager != null) manager.shutdown();
        deleteDir(new File(TEST_DIR));
    }

    @Test
    void testInitAndAppend() throws IOException {
        // 1. 初始化 (应该创建 Manifest 和第一个 Incr 文件)
        manager.init();

        File manifestFile = new File(TEST_DIR, "appendonly.aof.manifest");
        assertTrue(manifestFile.exists(), "Manifest file should be created");

        // 验证 Manifest 内容
        String manifestContent = Files.readString(manifestFile.toPath());
        assertTrue(manifestContent.contains("file appendonly.aof.1.incr.aof seq 1 type i"));

        File incrFile = new File(TEST_DIR, "appendonly.aof.1.incr.aof");
        assertTrue(incrFile.exists(), "Incr file should be created");

        // 2. 追加命令
        // SET key val
        RedisArray cmd = new RedisArray(new RedisMessage[]{
                new BulkString("SET"), new BulkString("key"), new BulkString("val")
        });
        manager.append(cmd);

        // 3. 验证 Incr 文件内容
        // 此时应该已经落盘 (因为是 ALWAYS 策略)
        String incrContent = Files.readString(incrFile.toPath());
        // RESP: *3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$3\r\nval\r\n
        assertTrue(incrContent.contains("*3\r\n$3\r\nSET"));
        assertTrue(incrContent.contains("$3\r\nkey"));
    }

    @Test
    void testRestartAndResume() throws IOException {
        // 1. 第一次运行
        manager.init();
        RedisArray cmd1 = new RedisArray(new RedisMessage[]{ new BulkString("SET"), new BulkString("k1"), new BulkString("v1") });
        manager.append(cmd1);
        manager.shutdown(); // 关闭，模拟重启

        // 2. 第二次运行 (重启)
        AofManager manager2 = new AofManager(config);
        manager2.init(); // 应该加载 Manifest，发现已经有 seq 1 了

        // 检查是否复用了同一个 Incr 文件，或者创建了新的？
        // Redis 7.0 策略：重启时通常不会立即滚动 Incr，除非触发 Rewrite。
        // 我们的简单实现：init() 逻辑里是 "如果 lastIncr 存在，就 open 它"。
        // 所以应该继续追加到 .1.incr.aof

        RedisArray cmd2 = new RedisArray(new RedisMessage[]{ new BulkString("SET"), new BulkString("k2"), new BulkString("v2") });
        manager2.append(cmd2);
        manager2.shutdown();

        // 3. 验证文件内容
        File incrFile = new File(TEST_DIR, "appendonly.aof.1.incr.aof");
        assertTrue(incrFile.exists());

        String content = Files.readString(incrFile.toPath());
        // 应该包含两次写入
        assertTrue(content.contains("k1"));
        assertTrue(content.contains("k2"));

        // 验证没有创建多余的文件
        File incrFile2 = new File(TEST_DIR, "appendonly.aof.2.incr.aof");
        assertFalse(incrFile2.exists(), "Should not create new incr file on simple restart");
    }

    // 递归删除
    private void deleteDir(File file) {
        if (file.isDirectory()) {
            File[] files = file.listFiles();
            if (files != null) for (File f : files) deleteDir(f);
        }
        file.delete();
    }
}
