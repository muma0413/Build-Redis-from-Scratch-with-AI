package org.muma.mini.redis.rdb;

import org.muma.mini.redis.common.RedisHash;
import org.muma.mini.redis.common.RedisList;
import org.muma.mini.redis.common.RedisSet;
import org.muma.mini.redis.common.RedisZSet;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

/**
 * RDB 反序列化器
 * 负责从 InputStream 解析 RDB 格式数据，还原为 Java 对象。
 */
public class RdbDecoder {

    private final DataInputStream in;

    public RdbDecoder(InputStream in) {
        // 使用 DataInputStream 方便读取 byte, int, long
        this.in = new DataInputStream(in);
    }

    /**
     * 读取一个字节 (0-255)
     */
    public int readByte() throws IOException {
        return in.readUnsignedByte();
    }

    /**
     * 读取指定长度的字节数组
     */
    public byte[] readBytes(int len) throws IOException {
        byte[] bytes = new byte[len];
        in.readFully(bytes);
        return bytes;
    }

    /**
     * 读取 RDB 长度编码
     *
     * @return 解析出的长度值
     */
    public long readLength() throws IOException {
        int b = in.readUnsignedByte();
        // 取高 2 位
        int type = (b & 0xC0) >> 6;

        if (type == 0) {
            // 00xxxxxx: 6位长度
            return b & 0x3F;
        } else if (type == 1) {
            // 01xxxxxx xxxxxxxx: 14位长度
            int next = in.readUnsignedByte();
            return ((b & 0x3F) << 8) | next;
        } else if (type == 2) {
            // 10xxxxxx: 32位长度 (后接4字节)
            // 忽略 b 的低6位，直接读后续 4 字节
            return in.readInt() & 0xFFFFFFFFL; // 转为无符号 long
        } else {
            // 11xxxxxx: 特殊编码 (整数/压缩)，暂不实现
            throw new IOException("Unsupported RDB length encoding type: " + type);
        }
    }

    // --- 对象解码 ---

    /**
     * 读取字符串对象
     */
    public byte[] readString() throws IOException {
        long len = readLength();
        if (len > Integer.MAX_VALUE) {
            throw new IOException("String too long: " + len);
        }
        return readBytes((int) len);
    }

    public String readStringUtf8() throws IOException {
        return new String(readString(), StandardCharsets.UTF_8);
    }

    /**
     * 读取 List 对象
     */
    public RedisList readList() throws IOException {
        long size = readLength();
        RedisList list = new RedisList();
        for (int i = 0; i < size; i++) {
            byte[] item = readString();
            list.rpush(item);
        }
        return list;
    }

    /**
     * 读取 Set 对象
     */
    public RedisSet readSet() throws IOException {
        long size = readLength();
        RedisSet set = new RedisSet();
        for (int i = 0; i < size; i++) {
            byte[] member = readString();
            set.add(member);
        }
        return set;
    }

    /**
     * 读取 Hash 对象
     */
    public RedisHash readHash() throws IOException {
        long size = readLength();
        RedisHash hash = new RedisHash();
        for (int i = 0; i < size; i++) {
            String key = readStringUtf8();
            byte[] val = readString();
            hash.put(key, val);
        }
        return hash;
    }

    /**
     * 读取 ZSet 对象
     */
    public RedisZSet readZSet() throws IOException {
        long size = readLength();
        RedisZSet zset = new RedisZSet();
        for (int i = 0; i < size; i++) {
            String member = readStringUtf8();
            // Score 是字符串格式存储的
            String scoreStr = readStringUtf8();
            double score;
            try {
                score = Double.parseDouble(scoreStr);
            } catch (NumberFormatException e) {
                throw new IOException("Invalid ZSet score: " + scoreStr);
            }
            zset.add(score, member);
        }
        return zset;
    }

    public long readLong() throws IOException {
        return in.readLong();
    }

    public int readInt() throws IOException {
        return in.readInt();
    }
}
