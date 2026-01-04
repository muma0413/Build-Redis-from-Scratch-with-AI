package org.muma.mini.redis.command.impl.set;

import org.muma.mini.redis.common.RedisSet;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class SInterCommand extends AbstractSetOperationCommand {
    @Override
    protected String getCommandName() {
        return "sinter";
    }

    @Override
    protected Set<ByteBuffer> compute(List<RedisSet> sets) {
        // 1. 快速失败
        for (RedisSet set : sets) {
            if (set == null || set.size() == 0) return new HashSet<>();
        }

        // 2. 找最小集合
        RedisSet base = sets.get(0);
        int baseIdx = 0;
        for (int i = 1; i < sets.size(); i++) {
            if (sets.get(i).size() < base.size()) {
                base = sets.get(i);
                baseIdx = i;
            }
        }

        Set<ByteBuffer> result = new HashSet<>();

        // 3. 遍历基准
        for (byte[] member : base.getAll()) {
            boolean allContain = true;
            for (int i = 0; i < sets.size(); i++) {
                if (i == baseIdx) continue;
                if (!sets.get(i).contains(member)) {
                    allContain = false;
                    break;
                }
            }
            if (allContain) {
                result.add(wrap(member));
            }
        }
        return result;
    }
}
