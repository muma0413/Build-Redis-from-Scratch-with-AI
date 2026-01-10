package org.muma.mini.redis.replication;

public enum ReplState {
    // --- As Slave ---
    NONE,               // 非 Slave (Master 模式)
    CONNECT,            // 正在连接 Master
    CONNECTING,         // TCP 连接中
    RECEIVE_PONG,       // 等待 PING 响应
    SEND_PORT,          // 发送 REPLCONF listening-port
    SEND_CAPA,          // 发送 REPLCONF capa (能力协商)
    SEND_PSYNC,         // 发送 PSYNC
    RECEIVE_PSYNC,      // 等待 PSYNC 响应
    TRANSFER,           // 正在接收 RDB
    CONNECTED,          // 全量同步完成，进入增量同步 (Command Stream)

    // --- As Master ---
    // Master 实际上没有全局状态，状态是针对每个 Slave 连接维护的
    // 这里主要用于 Slave 端的自身状态机
}
