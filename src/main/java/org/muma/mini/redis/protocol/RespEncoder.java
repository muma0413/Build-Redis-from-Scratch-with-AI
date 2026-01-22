package org.muma.mini.redis.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

import java.nio.charset.StandardCharsets;

/**
 * RESP 协议编码器 (Encoder)
 * <p>
 * 职责：将内部的 RedisMessage 对象转换为符合 RESP 协议规范的字节流。
 * 协议规范：
 * - Simple Strings: +OK\r\n
 * - Errors: -Error message\r\n
 * - Integers: :1000\r\n
 * - Bulk Strings: $6\r\nfoobar\r\n
 * - Arrays: *2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n
 * <p>
 * 特性：
 * - 支持递归编码 (Nested Arrays)，这是 SCAN/HSCAN 等命令必须的。
 * - 纯 Netty ByteBuf 操作，高性能。
 */
public class RespEncoder extends MessageToByteEncoder<RedisMessage> {

    private static final byte[] CRLF = "\r\n".getBytes(StandardCharsets.UTF_8);
    private static final byte[] NULL_BULK_STRING = "-1".getBytes(StandardCharsets.UTF_8);
    private static final byte[] NULL_ARRAY = "-1".getBytes(StandardCharsets.UTF_8);

    @Override
    protected void encode(ChannelHandlerContext ctx, RedisMessage msg, ByteBuf out) {
        // 委托给递归辅助方法
        writeTo(out, msg);
    }

    /**
     * 核心递归编码逻辑
     * 将任意 RedisMessage 写入 ByteBuf
     */
    private void writeTo(ByteBuf out, RedisMessage msg) {
        // 1. Simple String (+)
        if (msg instanceof SimpleString s) {
            out.writeByte('+');
            out.writeBytes(s.content().getBytes(StandardCharsets.UTF_8));
            out.writeBytes(CRLF);
        }
        // 2. Error (-)
        else if (msg instanceof ErrorMessage e) {
            out.writeByte('-');
            out.writeBytes(e.content().getBytes(StandardCharsets.UTF_8));
            out.writeBytes(CRLF);
        }
        // 3. Integer (:)
        else if (msg instanceof RedisInteger i) {
            out.writeByte(':');
            out.writeBytes(String.valueOf(i.value()).getBytes(StandardCharsets.UTF_8));
            out.writeBytes(CRLF);
        }
        // 4. Bulk String ($)
        else if (msg instanceof BulkString b) {
            out.writeByte('$');
            if (b.content() == null) {
                // Null Bulk String ($-1\r\n)
                out.writeBytes(NULL_BULK_STRING);
                out.writeBytes(CRLF);
            } else {
                // $<length>\r\n<data>\r\n
                out.writeBytes(String.valueOf(b.content().length).getBytes(StandardCharsets.UTF_8));
                out.writeBytes(CRLF);
                out.writeBytes(b.content());
                out.writeBytes(CRLF);
            }
        }
        // 5. Array (*)
        else if (msg instanceof RedisArray a) {
            out.writeByte('*');
            if (a.elements() == null) {
                // Null Array (*-1\r\n)
                out.writeBytes(NULL_ARRAY);
                out.writeBytes(CRLF);
            } else {
                // *<count>\r\n
                out.writeBytes(String.valueOf(a.elements().length).getBytes(StandardCharsets.UTF_8));
                out.writeBytes(CRLF);
                // 递归写入每个元素
                for (RedisMessage element : a.elements()) {
                    writeTo(out, element);
                }
            }
        }
        // 6. Unknown Type
        else {
            throw new IllegalArgumentException("Unsupported RedisMessage type: " + msg.getClass().getName());
        }
    }
}
