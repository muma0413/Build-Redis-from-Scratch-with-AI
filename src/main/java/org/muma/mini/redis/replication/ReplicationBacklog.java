package org.muma.mini.redis.replication;

import lombok.Getter;
import org.muma.mini.redis.protocol.RedisArray;
import org.muma.mini.redis.utils.RespCodecUtil;

import java.util.Arrays;

/**
 * 复制积压缓冲区 (Fixed Size Ring Buffer)
 * 用于支持 PSYNC 部分重同步。
 */
public class ReplicationBacklog {

    // 默认 1MB，生产环境通常更大 (如 100MB)
    private static final int DEFAULT_SIZE = 1024 * 1024;

    private final byte[] buffer;
    private final int capacity;

    // 全局 offset，对应 buffer[0] 的位置 (实际上是 buffer 头部的虚拟偏移)
    // 但为了计算简单，我们维护 global_offset (写入总字节数)
    @Getter
    private long masterReplOffset = 0;

    // 缓冲区中最早的数据对应的全局 offset
    // 因为是环形的，当写满一圈后，旧数据被覆盖，offsetFirst 就会增加
    // 有效范围: [masterReplOffset - contentSize, masterReplOffset]

    // 写指针 (在 buffer 中的索引)
    private int writeIdx = 0;

    public ReplicationBacklog() {
        this(DEFAULT_SIZE);
    }

    public ReplicationBacklog(int size) {
        this.capacity = size;
        this.buffer = new byte[size];
    }

    /**
     * 写入命令
     */
    public void write(RedisArray command) {
        byte[] bytes = RespCodecUtil.encode(command);
        writeBytes(bytes);
    }

    private void writeBytes(byte[] bytes) {
        // 环形写入
        for (byte b : bytes) {
            buffer[writeIdx] = b;
            writeIdx = (writeIdx + 1) % capacity;
        }
        masterReplOffset += bytes.length;
    }

    /**
     * 判断 slave 请求的 offset 是否在缓冲区内
     */
    public boolean isValidOffset(long offset) {
        // 有效数据的起始 offset
        // 如果 offset < capacity，起始就是 0
        // 如果 offset > capacity，起始就是 offset - capacity
        long minOffset = Math.max(0, masterReplOffset - capacity);

        return offset >= minOffset && offset <= masterReplOffset;
    }

    /**
     * 获取从 offset 开始的所有增量数据
     */
    public byte[] getBytesFrom(long offset) {
        if (!isValidOffset(offset)) return null;

        long diff = masterReplOffset - offset;
        if (diff == 0) return new byte[0]; // 追平了

        byte[] result = new byte[(int) diff];

        // 计算 offset 在 buffer 中的起始索引
        // 注意：buffer 是环形的。writeIdx 指向下一个要写的位置。
        // 当前 masterReplOffset 对应 writeIdx。
        // 所以 offset 对应的索引是：
        // writeIdx - diff (考虑环形回绕)
        int startIdx = (int) ((writeIdx - diff + capacity) % capacity);

        for (int i = 0; i < diff; i++) {
            result[i] = buffer[(startIdx + i) % capacity];
        }
        return result;
    }

}
