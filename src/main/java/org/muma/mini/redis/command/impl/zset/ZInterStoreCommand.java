package org.muma.mini.redis.command.impl.zset;

import org.muma.mini.redis.common.RedisZSet;

import java.util.List;

/**
 * ZINTERSTORE dest numkeys key [key ...] [WEIGHTS w1 w2] [AGGREGATE SUM|MIN|MAX]
 * <p>
 * 【时间复杂度 / Time Complexity】
 * O(N * K) + O(M * log(M))
 * - N: 最小集合的元素数量 (Smallest Set Size)
 * - K: 集合的总数 (Number of keys)
 * - M: 结果集的大小
 * <p>
 * 【算法优化 / Optimization】
 * 我们不应该遍历所有集合。
 * 交集的特性决定了：结果集的大小 <= 最小集合的大小。
 * <p>
 * 策略：
 * 1. 找到元素最少的那个集合 (基准集合 Base Set)。
 * 2. 只遍历这个基准集合。
 * 3. 检查基准集合中的元素是否存在于其他所有集合中。
 * - 如果存在，计算 Score。
 * - 如果任何一个集合不存在该元素，则该元素被丢弃。
 * <p>
 * 这种优化使得当有一个小集合和一个超大集合做交集时，性能极快！
 */
public class ZInterStoreCommand extends AbstractZStoreCommand {

    @Override
    protected String getCommandName() {
        return "zinterstore";
    }

    @Override
    protected RedisZSet compute(List<RedisZSet> srcSets, double[] weights, int aggregate) {
        RedisZSet result = new RedisZSet();

        // 1. 快速失败：交集性质
        // 如果任何一个源 Key 不存在(null)或为空，交集一定为空。
        for (RedisZSet src : srcSets) {
            if (src == null || src.size() == 0) return result;
        }

        // 2. 寻找基准集合 (最小的集合)
        // 复杂度优化关键点
        RedisZSet base = srcSets.get(0);
        int baseIdx = 0;
        for (int i = 1; i < srcSets.size(); i++) {
            if (srcSets.get(i).size() < base.size()) {
                base = srcSets.get(i);
                baseIdx = i;
            }
        }

        // 3. 遍历基准集合
        List<RedisZSet.ZSetEntry> baseEntries = base.range(0, -1);

        for (RedisZSet.ZSetEntry entry : baseEntries) {
            String member = entry.member();
            double score = entry.score() * weights[baseIdx];
            boolean keep = true;

            // 4. 检查该元素是否在其他所有集合中都存在
            for (int i = 0; i < srcSets.size(); i++) {
                if (i == baseIdx) continue; // 跳过基准自己

                RedisZSet other = srcSets.get(i);
                // getScore 是 O(1) 操作 (利用了 HashMap)
                // 如果只用 SkipList，这里就是 O(logN)
                Double otherScore = other.getScore(member);

                if (otherScore == null) {
                    keep = false; // 只要有一个集合没有，就丢弃
                    break;
                }

                // 执行聚合逻辑
                double weightedOtherScore = otherScore * weights[i];
                if (aggregate == AGGREGATE_SUM) {
                    score += weightedOtherScore;
                } else if (aggregate == AGGREGATE_MIN) {
                    score = Math.min(score, weightedOtherScore);
                } else { // MAX
                    score = Math.max(score, weightedOtherScore);
                }
            }

            // 5. 如果所有集合都有，则加入结果集
            if (keep) {
                result.add(score, member);
            }
        }

        return result;
    }
}
