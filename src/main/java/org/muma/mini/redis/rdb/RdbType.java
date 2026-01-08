package org.muma.mini.redis.rdb;

public class RdbType {

    // 对应 RedisData.getType()
    public static final int STRING = 0;
    public static final int LIST   = 1;
    public static final int SET    = 2;
    public static final int ZSET   = 3;
    public static final int HASH   = 4;

    // Redis 还有很多优化编码 (如 ZIPLIST, INTSET)，我们 Mini-Redis 暂时只支持基础编码。
    // 如果我们要支持 QuickList/IntSet 的原生 RDB 格式，需要定义更多：
    // public static final int LIST_QUICKLIST = 14;
    // public static final int HASH_ZIPLIST = 13;
    // public static final int SET_INTSET = 11;
    // public static final int ZSET_ZIPLIST = 12;

    // 为了简化，我们先把内存里的 QuickList/SkipList "拍平" 存储为基础类型 (LIST/ZSET)
    // 这样实现最简单，兼容性也好。
}
