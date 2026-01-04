package org.muma.mini.redis.store.structure;


import java.util.List;

public interface SetProvider {
    int add(byte[] member);

    int remove(byte[] member);

    boolean contains(byte[] member);

    int size();

    List<byte[]> getAll();

    // 随机弹出 (SPOP)
    byte[] pop();

    // 随机获取 (SRANDMEMBER)
    List<byte[]> randomMembers(int count);
}

