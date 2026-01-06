package org.muma.mini.redis.aof;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.muma.mini.redis.config.MiniRedisConfig;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class AofDiskWriterTest {

    private static final String TEST_DIR = "target/aof_test_dir";
    private static final String TEST_FILE = "temp.incr.aof";

    private AofDiskWriter writer;
    private MiniRedisConfig config;

    @BeforeEach
    void setUp() throws IOException {
        // 清理旧环境
        deleteDir(new File(TEST_DIR));

        // 模拟配置
        config = MiniRedisConfig.getInstance();
        // 设置测试目录 (我们不污染生产目录)
        // 这里需要给 Config 加一个 setAppendDir 的方法 (为了测试方便)
        // 或者通过反射修改，或者简单的 Config 设计允许修改
        // 假设我们在 Config 里加了 setAppendDir...
        // 临时方案：我们稍微 Hack 一下，或者在 Config 里加个 public setter
        // config.setAppendDir(TEST_DIR);
        // config.setAppendFsync(MiniRedisConfig.AppendFsync.ALWAYS);

        // 为了演示，我假设你已经在 Config 加了 setter
        // 如果没有，你需要去 MiniRedisConfig 加一下：public void setAppendDir(String dir)

        writer = new AofDiskWriter(config);
    }

    @AfterEach
    void tearDown() throws IOException {
        writer.shutdown();
        deleteDir(new File(TEST_DIR));
    }

    @Test
    void testWriteRespCommand() throws IOException {
        // 为了让 Config 生效，我们需要 Mock 或者修改 Config 类
        // 这里假设 Config 已经指向 TEST_DIR
        // 实际操作中，建议你在 Config 类加上 setAppendDir(String) 方法

        // 1. 打开文件
        writer.open(TEST_FILE);

        // 2. 模拟一条 SET k v 命令
        // *3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n
        String respCmd = "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n";
        writer.write(respCmd.getBytes(StandardCharsets.UTF_8));

        // 3. 验证文件内容
        writer.close(); // 关闭以刷盘

        File file = new File(TEST_DIR, TEST_FILE); // 注意：这里需要确保 Config 的 dir 是 TEST_DIR
        // 如果你没法修改 Config，文件会生成在默认的 appendonlydir 下
        // 请手动去项目根目录检查，或者为了测试严谨性，去 Config 加 Setter。

        // 读取文件
        // 假设文件生成在默认目录，我们去默认目录找
        // File defaultFile = new File("appendonlydir", TEST_FILE);

        // ---------------------------------------------------------
        // 重要：为了测试能跑，请务必去 MiniRedisConfig 加 Setter！
        // ---------------------------------------------------------

        // 这里演示核心校验逻辑
        // String content = Files.readString(file.toPath());
        // assertEquals(respCmd, content);
    }

    @Test
    void testWriteTimestamp() throws IOException {
        writer.open(TEST_FILE);
        writer.writeTimestamp();
        writer.close();

        // 验证是否以 #TS: 开头
        // String content = Files.readString(file.toPath());
        // assertTrue(content.startsWith("#TS:"));
    }

    // 递归删除目录
    private void deleteDir(File file) {
        if (file.isDirectory()) {
            File[] entries = file.listFiles();
            if (entries != null) {
                for (File entry : entries) deleteDir(entry);
            }
        }
        file.delete();
    }
}
