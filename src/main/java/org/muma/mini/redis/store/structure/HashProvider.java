package org.muma.mini.redis.store.structure;

import java.util.Map;

/**
 * Hash 底层数据结构策略接口
 */
public interface HashProvider {
    int put(String field, byte[] value);

    byte[] get(String field);

    int remove(String field);

    int size();

    Map<String, byte[]> toMap(); // 用于升级时的数据导出
}
