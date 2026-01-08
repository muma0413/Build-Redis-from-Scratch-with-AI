package org.muma.mini.redis.rdb;

import org.muma.mini.redis.common.RedisHash;
import org.muma.mini.redis.common.RedisList;
import org.muma.mini.redis.common.RedisSet;
import org.muma.mini.redis.common.RedisZSet;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

/**
 * RDB 序列化器
 * 负责将 Java 对象转换为 RDB 格式的字节流。
 * 遵循 Redis RDB 版本 9 协议规范 (简化版)。
 */
public class RdbEncoder {

    private final OutputStream out;

    public RdbEncoder(OutputStream out) {
        this.out = out;
    }

    /**
     * 写入一个字节
     */
    public void writeByte(int b) throws IOException {
        out.write(b);
    }

    /**
     * 写入字节数组 (原样写入，不带长度)
     */
    public void writeBytes(byte[] bytes) throws IOException {
        out.write(bytes);
    }

    /**
     * 写入 RDB 长度编码 (Length Encoding)
     * <p>
     * 规则:
     * - 00xxxxxx: len < 64 (1 byte)
     * - 01xxxxxx: len < 16384 (2 bytes)
     * - 10000000: len >= 16384 (5 bytes, 1 byte flag + 4 bytes len)
     */
    public void writeLength(long len) throws IOException {
        if (len < 64) {
            // 00xxxxxx
            out.write((int) (len & 0xFF));
        } else if (len < 16384) {
            // 01xxxxxx xxxxxxxx
            // 高 2 位是 01，剩下 14 位存长度
            // byte1: 01000000 | (len >> 8)
            // byte2: len & 0xFF
            int b1 = 0x40 | (int) ((len >> 8) & 0x3F);
            int b2 = (int) (len & 0xFF);
            out.write(b1);
            out.write(b2);
        } else {
            // 10000000 + 4 bytes (Big Endian in Redis RDB)
            // 注意：Redis RDB 的 32位/64位长度通常是大端序写入
            out.write(0x80);
            writeInt((int) len);
        }
    }

    /**
     * 写入 4 字节整数 (Big Endian)
     */
    private void writeInt(int v) throws IOException {
        out.write((v >>> 24) & 0xFF);
        out.write((v >>> 16) & 0xFF);
        out.write((v >>> 8) & 0xFF);
        out.write((v >>> 0) & 0xFF);
    }

    /**
     * 写入 8 字节长整数 (Big Endian)
     */
    public void writeLong(long v) throws IOException {
        out.write((int) (v >>> 56) & 0xFF);
        out.write((int) (v >>> 48) & 0xFF);
        out.write((int) (v >>> 40) & 0xFF);
        out.write((int) (v >>> 32) & 0xFF);
        out.write((int) (v >>> 24) & 0xFF);
        out.write((int) (v >>> 16) & 0xFF);
        out.write((int) (v >>> 8) & 0xFF);
        out.write((int) (v >>> 0) & 0xFF);
    }

    /**
     * 写入 Double (8 bytes IEEE 754)
     */
    public void writeDouble(double v) throws IOException {
        long bits = Double.doubleToLongBits(v);
        writeLong(bits);
    }

    // --- 对象编码 (Object Encoding) ---

    /**
     * 写入字符串对象 (String Encoding)
     * 格式: [Length][Content]
     */
    public void writeString(byte[] bytes) throws IOException {
        writeLength(bytes.length);
        writeBytes(bytes);
    }

    public void writeString(String str) throws IOException {
        writeString(str.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * 写入 List 对象
     * 格式: [Size][Item1][Item2]...
     */
    public void writeList(RedisList list) throws IOException {
        List<byte[]> items = list.range(0, -1);
        writeLength(items.size());
        for (byte[] item : items) {
            writeString(item);
        }
    }

    /**
     * 写入 Set 对象
     * 格式: [Size][Member1][Member2]...
     */
    public void writeSet(RedisSet set) throws IOException {
        List<byte[]> members = set.getAll();
        writeLength(members.size());
        for (byte[] member : members) {
            writeString(member);
        }
    }

    /**
     * 写入 Hash 对象
     * 格式: [Size][Key1][Value1][Key2][Value2]...
     */
    public void writeHash(RedisHash hash) throws IOException {
        Map<String, byte[]> map = hash.toMap();
        writeLength(map.size());
        for (Map.Entry<String, byte[]> entry : map.entrySet()) {
            writeString(entry.getKey());
            writeString(entry.getValue());
        }
    }

    /**
     * 写入 ZSet 对象
     * 格式: [Size][Member1][Score1][Member2][Score2]...
     * 注意：Redis RDB 这里的 Score 可能是字符串格式，也可能是浮点数。
     * 为了兼容性最好，Redis 早期版本 ZSet 存的是 String 格式的 Score (ASCII)。
     * Redis 5.0+ 有变化。为了最广泛的兼容性，我们把 Score 转成 String 存。
     * <p>
     * 格式变体:
     * 1. 存 Double (8 bytes) -> 更紧凑，但在文本协议里不直观。
     * 2. 存 String -> 兼容性好 (Redis 2.x - 4.x)。
     * <p>
     * 我们选择: String 格式的 Score。
     */
    public void writeZSet(RedisZSet zset) throws IOException {
        List<RedisZSet.ZSetEntry> entries = zset.range(0, -1);
        writeLength(entries.size());
        for (RedisZSet.ZSetEntry entry : entries) {
            writeString(entry.member());

            // Score 转 String
            double s = entry.score();
            String scoreStr = (s % 1 == 0) ? String.valueOf((long) s) : String.valueOf(s);
            writeString(scoreStr);
        }
    }
}
