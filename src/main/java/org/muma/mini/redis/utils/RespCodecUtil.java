package org.muma.mini.redis.utils;

import org.muma.mini.redis.protocol.BulkString;
import org.muma.mini.redis.protocol.RedisArray;
import org.muma.mini.redis.protocol.RedisMessage;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * RESP 协议编码工具类
 * 用于 AOF 重写时将 Command 对象序列化为字节流
 */
public class RespCodecUtil {

    private static final byte[] CRLF = "\r\n".getBytes(StandardCharsets.UTF_8);

    public static byte[] encode(RedisArray array) {
        // 使用 ByteArrayOutputStream 在内存中构建字节流
        // 预估大小：简单估算，避免频繁扩容。虽然不太准，但比默认 32 好。
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream(512)) {

            RedisMessage[] elements = array.elements();
            if (elements == null) {
                // *-1\r\n (Null Array)
                bos.write('*');
                bos.write("-1".getBytes(StandardCharsets.UTF_8));
                bos.write(CRLF);
                return bos.toByteArray();
            }

            // *<count>\r\n
            bos.write('*');
            bos.write(String.valueOf(elements.length).getBytes(StandardCharsets.UTF_8));
            bos.write(CRLF);

            for (RedisMessage msg : elements) {
                if (msg instanceof BulkString bs) {
                    bos.write('$');
                    if (bs.content() == null) {
                        // $-1\r\n (Null Bulk String)
                        bos.write("-1".getBytes(StandardCharsets.UTF_8));
                        bos.write(CRLF);
                    } else {
                        // $<length>\r\n<data>\r\n
                        bos.write(String.valueOf(bs.content().length).getBytes(StandardCharsets.UTF_8));
                        bos.write(CRLF);
                        bos.write(bs.content());
                        bos.write(CRLF);
                    }
                } else {
                    // 理论上 Rewrite 生成的 Command 参数都是 BulkString
                    // 如果出现了 SimpleString 或 Integer，说明 objectToCommand 逻辑有误
                    throw new IllegalArgumentException("Unsupported type in AOF rewrite: " + msg.getClass());
                }
            }
            return bos.toByteArray();
        } catch (IOException e) {
            // ByteArrayOutputStream 不会抛出 IO 异常，这里只是为了编译通过
            throw new RuntimeException("Should not happen", e);
        }
    }
}
