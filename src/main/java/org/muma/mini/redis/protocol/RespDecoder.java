package org.muma.mini.redis.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ReplayingDecoder;

import java.util.List;

/**
 * RESP 协议解码器
 * 状态机逻辑由 ReplayingDecoder 自动处理
 */
public class RespDecoder extends ReplayingDecoder<Void> {

    // RESP 协议常量
    private static final byte PLUS_BYTE = '+';
    private static final byte MINUS_BYTE = '-';
    private static final byte COLON_BYTE = ':';
    private static final byte DOLLAR_BYTE = '$';
    private static final byte ASTERISK_BYTE = '*';

    // 回车换行
    private static final byte CR = '\r';
    private static final byte LF = '\n';

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        // 1. 读取类型标识字节
        byte typeByte = in.readByte();

        // 2. 根据类型分发处理
        switch (typeByte) {
            case PLUS_BYTE -> out.add(new SimpleString(readLine(in)));
            case MINUS_BYTE -> out.add(new ErrorMessage(readLine(in)));
            case COLON_BYTE -> out.add(new RedisInteger(readLong(in)));
            case DOLLAR_BYTE -> out.add(decodeBulkString(in));
            case ASTERISK_BYTE -> out.add(decodeArray(in));
            default -> throw new IllegalStateException("Unknown RESP type byte: " + (char) typeByte);
        }
    }

    // 解析 BulkString: $<length>\r\n<data>\r\n
    private BulkString decodeBulkString(ByteBuf in) {
        int length = (int) readLong(in);
        if (length == -1) {
            return new BulkString((byte[]) null); // Null Bulk String
        }

        byte[] content = new byte[length];
        in.readBytes(content);

        // 读取末尾的 CRLF
        readGenericCRLF(in);

        return new BulkString(content);
    }

    // 解析 Array: *<count>\r\n<element1>...<elementN>
    private RedisArray decodeArray(ByteBuf in) {
        int count = (int) readLong(in);
        if (count == -1) {
            return new RedisArray(null); // Null Array
        }

        RedisMessage[] elements = new RedisMessage[count];
        for (int i = 0; i < count; i++) {
            // 递归调用 decode (注意：这里需要手动触发一次子解析)
            // 在 ReplayingDecoder 中，我们可以直接递归调用 decode 逻辑，
            // 但更好的方式是利用 Netty 的 decode 循环，或者简单的递归方法。
            // 简单起见，我们将 parse 逻辑抽取出来，或者利用 checkpoint。
            // 为了演示清晰，我们这里手动模拟递归读取一个完整对象。
            elements[i] = readNextObject(in);
        }
        return new RedisArray(elements);
    }

    // 辅助：递归读取下一个完整的 RedisMessage
    // 注意：在 ReplayingDecoder 中，如果数据不够，这里会抛出 Signal 异常并回滚索引
    private RedisMessage readNextObject(ByteBuf in) {
        byte type = in.readByte();
        return switch (type) {
            case PLUS_BYTE -> new SimpleString(readLine(in));
            case MINUS_BYTE -> new ErrorMessage(readLine(in));
            case COLON_BYTE -> new RedisInteger(readLong(in));
            case DOLLAR_BYTE -> decodeBulkString(in);
            case ASTERISK_BYTE -> decodeArray(in);
            default -> throw new IllegalStateException("Unknown type: " + (char) type);
        };
    }

    // 辅助：读取一行字符串（即读取到 \r\n 为止）
    private String readLine(ByteBuf in) {
        StringBuilder sb = new StringBuilder();
        while (true) {
            byte b = in.readByte();
            if (b == CR) {
                byte next = in.readByte();
                if (next == LF) {
                    break;
                } else {
                    sb.append((char) b);
                    sb.append((char) next);
                }
            } else {
                sb.append((char) b);
            }
        }
        return sb.toString();
    }

    // 辅助：读取并解析长整型
    private long readLong(ByteBuf in) {
        String s = readLine(in);
        return Long.parseLong(s);
    }

    // 辅助：跳过 CRLF
    private void readGenericCRLF(ByteBuf in) {
        byte b1 = in.readByte();
        byte b2 = in.readByte();
        if (b1 != CR || b2 != LF) {
            throw new IllegalStateException("Expected CRLF");
        }
    }
}
