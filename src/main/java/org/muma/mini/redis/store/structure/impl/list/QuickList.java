package org.muma.mini.redis.store.structure.impl.list;

import org.muma.mini.redis.store.structure.ListProvider;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class QuickList implements ListProvider {

    private QuickListNode head;
    private QuickListNode tail;
    private int count; // 总元素数

    public QuickList() {
        this.head = this.tail = null;
        this.count = 0;
    }

    @Override
    public void lpush(byte[] element) {
        // 如果为空，或 head 已满，则新建 head
        if (head == null || head.isFull()) {
            QuickListNode newNode = new QuickListNode();
            if (head == null) {
                head = tail = newNode;
            } else {
                newNode.next = head;
                head.prev = newNode;
                head = newNode;
            }
        }
        head.addFirst(element);
        count++;
    }

    @Override
    public void rpush(byte[] element) {
        // 如果为空，或 tail 已满，则新建 tail
        if (tail == null || tail.isFull()) {
            QuickListNode newNode = new QuickListNode();
            if (tail == null) {
                head = tail = newNode;
            } else {
                tail.next = newNode;
                newNode.prev = tail;
                tail = newNode;
            }
        }
        tail.addLast(element);
        count++;
    }

    @Override
    public byte[] lpop() {
        if (count == 0) return null;

        byte[] val = head.removeFirst();
        count--;

        // 如果 Node 空了，删除该 Node (GC)
        if (head.isEmpty()) {
            QuickListNode next = head.next;
            if (next != null) {
                next.prev = null;
                head.next = null; // help GC
                head = next;
            } else {
                head = tail = null;
            }
        }
        return val;
    }

    @Override
    public byte[] rpop() {
        if (count == 0) return null;

        byte[] val = tail.removeLast();
        count--;

        if (tail.isEmpty()) {
            QuickListNode prev = tail.prev;
            if (prev != null) {
                prev.next = null;
                tail.prev = null; // help GC
                tail = prev;
            } else {
                head = tail = null;
            }
        }
        return val;
    }

    @Override
    public int size() {
        return count;
    }

    @Override
    public List<byte[]> range(long start, long stop) {
        if (count == 0) return Collections.emptyList();

        // 索引归一化
        if (start < 0) start = count + start;
        if (stop < 0) stop = count + stop;
        if (start < 0) start = 0;
        if (start > stop || start >= count) return Collections.emptyList();
        if (stop >= count) stop = count - 1;

        List<byte[]> result = new ArrayList<>((int)(stop - start + 1));

        // 遍历逻辑：找到 start 所在的 Node，然后顺序收集
        // 优化：根据 start 靠近 head 还是 tail 决定从哪边开始找 Node
        // 这里简单实现：从 head 开始

        QuickListNode current = head;
        int accumulated = 0; // 当前遍历过的元素总数

        // 1. Skip nodes until we reach start
        while (current != null) {
            int nodeSize = current.size();
            if (accumulated + nodeSize > start) {
                break; // start 就在这个节点里
            }
            accumulated += nodeSize;
            current = current.next;
        }

        // 2. Collect elements
        // 此时 accumulated <= start
        int offsetInNode = (int) (start - accumulated);
        long needed = stop - start + 1;

        while (needed > 0 && current != null) {
            List<byte[]> zl = current.getZipList();
            for (int i = offsetInNode; i < zl.size() && needed > 0; i++) {
                result.add(zl.get(i));
                needed--;
            }
            // 跨节点了，下一个节点从 0 开始取
            offsetInNode = 0;
            current = current.next;
        }

        return result;
    }

    @Override
    public byte[] index(long index) {
        if (count == 0) return null;
        if (index < 0) index = count + index;
        if (index < 0 || index >= count) return null;

        // 定位 Node
        QuickListNode current = head;
        int accumulated = 0;

        while (current != null) {
            int nodeSize = current.size();
            if (accumulated + nodeSize > index) {
                // 找到了
                int offset = (int) (index - accumulated);
                return current.getZipList().get(offset);
            }
            accumulated += nodeSize;
            current = current.next;
        }
        return null;
    }

    @Override
    public void set(long index, byte[] element) {
        // 逻辑同 index，找到后 set
        if (count == 0) throw new IndexOutOfBoundsException();
        if (index < 0) index = count + index;
        if (index < 0 || index >= count) throw new IndexOutOfBoundsException();

        QuickListNode current = head;
        int accumulated = 0;

        while (current != null) {
            int nodeSize = current.size();
            if (accumulated + nodeSize > index) {
                int offset = (int) (index - accumulated);
                current.getZipList().set(offset, element);
                return;
            }
            accumulated += nodeSize;
            current = current.next;
        }
    }

    @Override
    public int insert(boolean before, byte[] pivot, byte[] value) {
        QuickListNode current = head;
        // 1. 寻找 pivot
        // 遍历所有 Node
        while (current != null) {
            List<byte[]> zl = current.getZipList();
            // 遍历 Node 内的 ZipList
            for (int i = 0; i < zl.size(); i++) {
                byte[] item = zl.get(i);
                // 比较 byte[] 内容 (这里假设有 Arrays.equals 或者简单的循环比较)
                if (java.util.Arrays.equals(item, pivot)) {
                    // 找到了 pivot，位置是 current 节点的第 i 个
                    if (before) {
                        zl.add(i, value);
                    } else {
                        zl.add(i + 1, value);
                    }

                    // 检查该 Node 是否过大，如果过大需要分裂 (Split)
                    // 为了简化，Mini-Redis 暂时允许临时超过 MAX_SIZE
                    // 或者在这里简单判断一下:
                    if (current.isFull()) {
                        // TODO: 触发分裂逻辑 (Split Node)
                        // 这是一个极其复杂的操作，为了不让代码失控，
                        // 我们暂时允许 Node 膨胀，或者简单的把一半移到新 Node
                        splitNode(current);
                    }

                    count++;
                    return count;
                }
            }
            current = current.next;
        }
        return -1; // pivot not found
    }

    // 简单的节点分裂逻辑
    private void splitNode(QuickListNode node) {
        List<byte[]> zl = node.getZipList();
        int mid = zl.size() / 2;

        List<byte[]> rightPart = new ArrayList<>(zl.subList(mid, zl.size()));
        zl.subList(mid, zl.size()).clear(); // 左边保留前半部分

        QuickListNode newNode = new QuickListNode();
        newNode.getZipList().addAll(rightPart);

        // 链入链表: node -> newNode -> node.next
        newNode.next = node.next;
        newNode.prev = node;
        if (node.next != null) node.next.prev = newNode;
        node.next = newNode;

        if (node == tail) tail = newNode;
    }

    @Override
    public int remove(long count, byte[] element) {
        int removed = 0;

        if (count == 0) {
            // 移除所有：从头遍历
            QuickListNode current = head;
            while (current != null) {
                // 使用迭代器安全删除
                java.util.Iterator<byte[]> it = current.getZipList().iterator();
                while (it.hasNext()) {
                    if (java.util.Arrays.equals(it.next(), element)) {
                        it.remove();
                        removed++;
                        this.count--;
                    }
                }
                // 如果 Node 空了，删除 Node
                if (current.isEmpty()) {
                    QuickListNode next = current.next;
                    removeNode(current);
                    current = next;
                } else {
                    current = current.next;
                }
            }
        } else if (count > 0) {
            // 从头往尾删 count 个
            QuickListNode current = head;
            while (current != null && removed < count) {
                java.util.Iterator<byte[]> it = current.getZipList().iterator();
                while (it.hasNext() && removed < count) {
                    if (java.util.Arrays.equals(it.next(), element)) {
                        it.remove();
                        removed++;
                        this.count--;
                    }
                }
                if (current.isEmpty()) {
                    QuickListNode next = current.next;
                    removeNode(current);
                    current = next;
                } else {
                    current = current.next;
                }
            }
        } else {
            // count < 0: 从尾往头删 |count| 个
            long limit = Math.abs(count);
            QuickListNode current = tail;
            while (current != null && removed < limit) {
                // 倒序遍历 ZipList
                List<byte[]> zl = current.getZipList();
                for (int i = zl.size() - 1; i >= 0 && removed < limit; i--) {
                    if (java.util.Arrays.equals(zl.get(i), element)) {
                        zl.remove(i);
                        removed++;
                        this.count--;
                    }
                }
                if (current.isEmpty()) {
                    QuickListNode prev = current.prev;
                    removeNode(current);
                    current = prev;
                } else {
                    current = current.prev;
                }
            }
        }
        return removed;
    }

    private void removeNode(QuickListNode node) {
        if (node.prev != null) node.prev.next = node.next;
        else head = node.next;

        if (node.next != null) node.next.prev = node.prev;
        else tail = node.prev;

        node.prev = node.next = null; // GC
    }

    @Override
    public void trim(long start, long stop) {
        if (count == 0) return;

        // 归一化索引
        if (start < 0) start = count + start;
        if (stop < 0) stop = count + stop;
        if (start < 0) start = 0;
        if (stop >= count) stop = count - 1;

        if (start > stop) {
            // 清空所有
            head = tail = null;
            count = 0;
            return;
        }

        // 1. 定位新的 Head 所在的 Node 和 偏移量
        // 2. 定位新的 Tail 所在的 Node 和 偏移量
        // 3. 删除中间不需要的 Node
        // 4. 对新 Head/Tail Node 进行裁剪

        // 这种实现非常复杂，且极易出错。
        // 这里提供一个 "偷懒但正确" 的实现：
        // 利用 removeFirst / removeLast 循环删除不需要的元素。
        // 虽然效率 O(N)，但对于 Mini-Redis 绝对安全可靠。

        long leftToRemove = start;
        long rightToRemove = count - 1 - stop;

        for (int i = 0; i < leftToRemove; i++) {
            lpop(); // 复用 lpop，它会自动处理 Node 删除
        }
        for (int i = 0; i < rightToRemove; i++) {
            rpop(); // 复用 rpop
        }
    }



}
