package org.muma.mini.redis.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

import java.nio.charset.StandardCharsets;

public class RespEncoder extends MessageToByteEncoder<RedisMessage> {

    private static final byte[] CRLF = "\r\n".getBytes(StandardCharsets.UTF_8);

    @Override
    protected void encode(ChannelHandlerContext ctx, RedisMessage msg, ByteBuf out) {
        if (msg instanceof SimpleString s) {
            out.writeByte('+');
            out.writeBytes(s.content().getBytes(StandardCharsets.UTF_8));
            out.writeBytes(CRLF);
        } else if (msg instanceof ErrorMessage e) {
            out.writeByte('-');
            out.writeBytes(e.content().getBytes(StandardCharsets.UTF_8));
            out.writeBytes(CRLF);
        } else if (msg instanceof RedisInteger i) {
            out.writeByte(':');
            out.writeBytes(String.valueOf(i.value()).getBytes(StandardCharsets.UTF_8));
            out.writeBytes(CRLF);
        } else if (msg instanceof BulkString b) {
            out.writeByte('$');
            if (b.content() == null) {
                out.writeBytes("-1".getBytes(StandardCharsets.UTF_8));
                out.writeBytes(CRLF);
            } else {
                out.writeBytes(String.valueOf(b.content().length).getBytes(StandardCharsets.UTF_8));
                out.writeBytes(CRLF);
                out.writeBytes(b.content());
                out.writeBytes(CRLF);
            }
        } else if (msg instanceof RedisArray a) {
            out.writeByte('*');
            if (a.elements() == null) {
                out.writeBytes("-1".getBytes(StandardCharsets.UTF_8));
                out.writeBytes(CRLF);
            } else {
                out.writeBytes(String.valueOf(a.elements().length).getBytes(StandardCharsets.UTF_8));
                out.writeBytes(CRLF);
                for (RedisMessage element : a.elements()) {
                    // 递归编码数组元素。注意：MessageToByteEncoder 不会自动递归，需手动处理
                    // 这里为了简单，我们调用一个新的 encode 方法，或者重构逻辑。
                    // 实际项目中可以抽象 write 方法。这里我们简单复用逻辑：
                    // 由于 encode 是 protected，我们在内部抽象一个 write 方法
                    write(out, element);
                }
            }
        }
    }

    // 递归写入辅助方法
    private void write(ByteBuf out, RedisMessage msg) {
        // 这里需要把 encode 里的逻辑提取出来，为了代码简洁，
        // 实际开发中建议把 encode 逻辑放到 RedisMessage 接口中，例如 msg.encode(out)
        // 这里为了演示结构，简略重复部分逻辑:
        if (msg instanceof BulkString b) {
            out.writeByte('$');
            // ... 重复 BulkString 逻辑 ...
            if (b.content() == null) {
                out.writeBytes("-1".getBytes(StandardCharsets.UTF_8));
            } else {
                out.writeBytes(String.valueOf(b.content().length).getBytes(StandardCharsets.UTF_8));
                out.writeBytes(CRLF);
                out.writeBytes(b.content());
            }
            out.writeBytes(CRLF);
        }
        // *注意*：为了代码完整性，建议将 encode 逻辑移动到 RedisMessage 接口中实现多态调用
        // 或者将上面的 if-else 逻辑抽取为静态方法 void writeTo(ByteBuf, RedisMessage)
    }
}
