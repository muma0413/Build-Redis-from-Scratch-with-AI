package org.muma.mini.redis.aof;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.muma.mini.redis.config.MiniRedisConfig;
import org.muma.mini.redis.protocol.BulkString;
import org.muma.mini.redis.protocol.RedisArray;
import org.muma.mini.redis.protocol.RedisMessage;
import org.muma.mini.redis.store.StorageEngine;
import org.muma.mini.redis.store.impl.MemoryStorageEngine;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AofManagerTest {

    private static final String TEST_DIR = "target/aof_manager_test";
    private MiniRedisConfig config;
    private StorageEngine storage;
    private AofManager manager;

    @BeforeEach
    void setUp() throws IOException {
        // 清理环境
        deleteDir(new File(TEST_DIR));

        config = MiniRedisConfig.getInstance();
        config.setAppendDir(TEST_DIR);
        config.setAppendOnly(true); // 必须开启
        config.setAppendFsync(MiniRedisConfig.AppendFsync.ALWAYS); // 方便测试立即刷盘

        // 【新增】初始化 StorageEngine
        storage = new MemoryStorageEngine();

        // 【修改】传入 storage
        manager = new AofManager(config, storage);
    }

    @AfterEach
    void tearDown() {
        if (manager != null) manager.shutdown();
        deleteDir(new File(TEST_DIR));
    }

    @Test
    void testInitAndAppend() throws IOException {
        // 1. 初始化
        manager.init();

        File manifestFile = new File(TEST_DIR, "appendonly.aof.manifest");
        assertTrue(manifestFile.exists(), "Manifest file should be created");

        String manifestContent = Files.readString(manifestFile.toPath());
        assertTrue(manifestContent.contains("file appendonly.aof.1.incr.aof seq 1 type i"));

        File incrFile = new File(TEST_DIR, "appendonly.aof.1.incr.aof");
        assertTrue(incrFile.exists(), "Incr file should be created");

        // 2. 追加命令
        RedisArray cmd = new RedisArray(new RedisMessage[]{
                new BulkString("SET"), new BulkString("key"), new BulkString("val")
        });
        manager.append(cmd);

        // 3. 验证内容
        // 等待一下刷盘（虽然是 ALWAYS，但 diskWriter 是异步线程）
        try { Thread.sleep(50); } catch (InterruptedException ignored) {}

        String incrContent = Files.readString(incrFile.toPath());
        assertTrue(incrContent.contains("*3\r\n$3\r\nSET"));
        assertTrue(incrContent.contains("$3\r\nkey"));
    }

    @Test
    void testRestartAndResume() throws IOException {
        // 1. 第一次运行
        manager.init();
        RedisArray cmd1 = new RedisArray(new RedisMessage[]{ new BulkString("SET"), new BulkString("k1"), new BulkString("v1") });
        manager.append(cmd1);

        try { Thread.sleep(50); } catch (InterruptedException ignored) {}
        manager.shutdown();

        // 2. 第二次运行 (重启)
        // 【修改】传入 storage
        AofManager manager2 = new AofManager(config, storage);
        manager2.init();

        RedisArray cmd2 = new RedisArray(new RedisMessage[]{ new BulkString("SET"), new BulkString("k2"), new BulkString("v2") });
        manager2.append(cmd2);

        try { Thread.sleep(50); } catch (InterruptedException ignored) {}
        manager2.shutdown();

        // 3. 验证
        File incrFile = new File(TEST_DIR, "appendonly.aof.1.incr.aof");
        assertTrue(incrFile.exists());

        String content = Files.readString(incrFile.toPath());
        assertTrue(content.contains("k1"));
        assertTrue(content.contains("k2"));

        File incrFile2 = new File(TEST_DIR, "appendonly.aof.2.incr.aof");
        assertFalse(incrFile2.exists());
    }

    private void deleteDir(File file) {
        if (file.isDirectory()) {
            File[] files = file.listFiles();
            if (files != null) for (File f : files) deleteDir(f);
        }
        file.delete();
    }
}
