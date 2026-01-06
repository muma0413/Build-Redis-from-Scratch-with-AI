package org.muma.mini.redis.aof;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class AofManifestTest {

    @Test
    void testEncodeAndDecode() {
        // 1. 构建 Manifest
        AofManifest manifest = new AofManifest();

        // 模拟第一次 Rewrite 后的状态
        manifest.setBaseAof("appendonly.aof.1.base.aof", 1);
        manifest.addIncrAof("appendonly.aof.1.incr.aof", 1); // 刚开始重写时可能是同序号
        manifest.addIncrAof("appendonly.aof.2.incr.aof", 2); // 新的增量

        // 2. 编码
        String content = manifest.encode();
        System.out.println("Encoded Manifest:\n" + content);

        // 预期内容
        assertTrue(content.contains("file appendonly.aof.1.base.aof seq 1 type b"));
        assertTrue(content.contains("file appendonly.aof.2.incr.aof seq 2 type i"));

        // 3. 解码
        AofManifest decoded = AofManifest.decode(content);

        // 4. 验证数据
        assertNotNull(decoded.getBaseAof());
        assertEquals("appendonly.aof.1.base.aof", decoded.getBaseAof().filename);
        assertEquals(1, decoded.getBaseAof().seq);

        assertEquals(2, decoded.getIncrAofs().size());
        assertEquals("appendonly.aof.2.incr.aof", decoded.getIncrAofs().get(1).filename);

        // 验证 seq 恢复
        assertEquals(2, decoded.getCurrentSeq());
        // 下一个应该是 3
        assertEquals(3, decoded.nextSeq());
    }

    @Test
    void testEmptyDecode() {
        AofManifest m = AofManifest.decode("");
        assertNull(m.getBaseAof());
        assertEquals(0, m.getIncrAofs().size());
        assertEquals(0, m.getCurrentSeq());
    }
}
