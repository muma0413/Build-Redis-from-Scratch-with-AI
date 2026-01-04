package org.muma.mini.redis.command.impl.set;

import org.muma.mini.redis.common.RedisSet;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class SUnionCommand extends AbstractSetOperationCommand {
    @Override
    protected String getCommandName() {
        return "sunion";
    }

    @Override
    protected Set<ByteBuffer> compute(List<RedisSet> sets) {
        Set<ByteBuffer> result = new HashSet<>();
        for (RedisSet set : sets) {
            if (set == null) continue;
            for (byte[] member : set.getAll()) {
                result.add(wrap(member));
            }
        }
        return result;
    }
}
