package org.muma.mini.redis.command.impl.zset;

import org.muma.mini.redis.common.RedisZSet;

import java.util.List;

/**
 * ZUNIONSTORE dest numkeys key [key ...] [WEIGHTS w1 w2] [AGGREGATE SUM|MIN|MAX]
 * <p>
 * 【时间复杂度 / Time Complexity】
 * O(N) + O(M * log(M))
 * - N: 所有源集合的元素总数 (需要遍历读取)
 * - M: 结果集的元素数量
 * - log(M): 每次插入结果集(SkipList)的成本
 * <p>
 * 【性能风险】
 * 如果源集合非常大，这个操作会非常慢，并且消耗大量 CPU 进行 SkipList 的插入排序。
 * 属于 O(N) 级别的重型操作，生产环境需谨慎。
 */
public class ZUnionStoreCommand extends AbstractZStoreCommand {

    @Override
    protected String getCommandName() {
        return "zunionstore";
    }

    @Override
    protected RedisZSet compute(List<RedisZSet> srcSets, double[] weights, int aggregate) {
        RedisZSet result = new RedisZSet();

        for (int i = 0; i < srcSets.size(); i++) {
            RedisZSet src = srcSets.get(i);
            if (src == null) continue; // 忽略不存在的 Key

            double weight = weights[i];

            // 遍历源集合所有元素
            // range(0, -1) 会获取全量数据，内存消耗较大。
            // 优化思路：底层提供 iterator() 方法，避免一次性拷贝 List。
            // 但为了代码简洁，Mini-Redis 暂用 range。
            List<RedisZSet.ZSetEntry> entries = src.range(0, -1);

            for (RedisZSet.ZSetEntry entry : entries) {
                // 这里的 merge 逻辑：
                // 1. 如果 member 不存在，直接插入 (score * weight)
                // 2. 如果已存在，根据 AGGREGATE (SUM/MIN/MAX) 更新 score
                result.merge(entry, weight, aggregate);
            }
        }
        return result;
    }

    @Override
    public boolean isWrite() {
        return true;
    }
}
