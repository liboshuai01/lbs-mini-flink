package cn.liboshuai.flink.runtime.tasks.mailbox;

import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.ReentrantLock;

public class MiniInputGate {

    private final Queue<String> queue = new ArrayDeque<>();
    private final ReentrantLock lock = new ReentrantLock();

    // 这一步是 Flink 高效的关键：如果当前没数据，就给调用者一个 Future
    // 当有数据写入时，我们 complete 这个 Future，唤醒主线程
    private CompletableFuture<Void> availabilityFuture = new CompletableFuture<>();

    /**
     * [Netty 线程调用] 写入数据
     */
    public void pushData(String data) {
        lock.lock();
        try {
            queue.add(data);
            // 如果有线程（Task线程）正在等待数据（future未完成），现在由于数据来了，不仅要由未完成变为完成
            // 还需要重置一个新的 Future 给下一次等待用？
            // 在 Flink 中，一旦 future 完成，说明"现在有数据了"。
            if (!availabilityFuture.isDone()) {
                availabilityFuture.complete(null);
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * [Task 线程调用] 尝试获取数据
     *
     * @return 数据，如果为空则返回 null
     */
    public String pollNext() {
        lock.lock();
        try {
            String data = queue.poll();
            if (queue.isEmpty()) {
                // 如果队列空了，重置 future，表示"目前不可用"
                // 下次 Netty 写入时会再次 complete 它
                if (availabilityFuture.isDone()) {
                    availabilityFuture = new CompletableFuture<>();
                }
            }
            return data;
        } finally {
            lock.unlock();
        }
    }

    /**
     * [Task 线程调用] 获取可用性 Future
     * 只有当 InputGate 为空时，Task 才会关心这个 Future
     */
    public CompletableFuture<Void> getAvailableFuture() {
        lock.lock();
        try {
            return availabilityFuture;
        } finally {
            lock.unlock();
        }
    }
}
