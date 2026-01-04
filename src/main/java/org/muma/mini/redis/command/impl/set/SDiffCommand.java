package org.muma.mini.redis.command.impl.set;

import org.muma.mini.redis.common.RedisSet;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class SDiffCommand extends AbstractSetOperationCommand {
    @Override
    protected String getCommandName() {
        return "sdiff";
    }

    @Override
    protected Set<ByteBuffer> compute(List<RedisSet> sets) {
        // 第一个集合是基准
        RedisSet first = sets.get(0);
        if (first == null || first.size() == 0) return new HashSet<>();

        // 初始化结果集为第一个集合的所有元素
        Set<ByteBuffer> result = new HashSet<>();
        for (byte[] member : first.getAll()) {
            result.add(wrap(member));
        }

        // 遍历后续集合，执行移除
        for (int i = 1; i < sets.size(); i++) {
            RedisSet other = sets.get(i);
            if (other == null) continue;

            // 优化：如果 result 已经空了，提前结束
            if (result.isEmpty()) break;

            // 遍历小的集合更高效？
            // 策略：遍历 other，看 result 里有没有。
            // 或者是 result 里遍历，看 other 里有没有。
            // 这里简单处理：直接 removeAll (如果用 Set 接口的话)
            // 手动实现：
            for (byte[] member : other.getAll()) {
                result.remove(wrap(member));
            }
        }
        return result;
    }
}
