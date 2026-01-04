package org.muma.mini.redis.store.structure.impl.list;

import java.util.ArrayList;
import java.util.List;

/**
 * QuickList 的节点，内部包裹一个 "ZipList" (ArrayList)
 */
public class QuickListNode {
    // 双向指针
    public QuickListNode prev;
    public QuickListNode next;

    // 内部数据容器 (模拟 ZipList)
    private final List<byte[]> zipList;

    // 配置：每个节点最大容纳多少元素 (模拟 list-max-ziplist-size)
    // Redis 默认可能是 8KB 或 数量限制，这里我们简单用数量限制
    private static final int MAX_SIZE = 8;

    public QuickListNode() {
        this.zipList = new ArrayList<>(MAX_SIZE);
    }

    public boolean isFull() {
        return zipList.size() >= MAX_SIZE;
    }

    public boolean isEmpty() {
        return zipList.isEmpty();
    }

    public void addFirst(byte[] val) {
        zipList.add(0, val);
    }

    public void addLast(byte[] val) {
        zipList.add(val);
    }

    public byte[] removeFirst() {
        return zipList.remove(0);
    }

    public byte[] removeLast() {
        return zipList.remove(zipList.size() - 1);
    }

    // 获取内部数据用于遍历
    public List<byte[]> getZipList() {
        return zipList;
    }

    public int size() {
        return zipList.size();
    }
}
