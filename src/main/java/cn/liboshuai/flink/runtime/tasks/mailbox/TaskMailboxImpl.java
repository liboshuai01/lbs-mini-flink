package cn.liboshuai.flink.runtime.tasks.mailbox;

import lombok.extern.slf4j.Slf4j;

import java.util.Optional;
import java.util.PriorityQueue;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 邮箱的实现类。
 * 修改点：使用 PriorityQueue 替代了 ArrayDeque，以支持优先级调度。
 */
@Slf4j
public class TaskMailboxImpl implements TaskMailbox {

    // 核心锁
    private final ReentrantLock lock = new ReentrantLock();

    // 条件变量：队列不为空
    private final Condition notEmpty = lock.newCondition();

    // 修改点: 使用 PriorityQueue 替代 ArrayDeque
    // 依赖 Mail 类的 compareTo 方法进行排序
    private final PriorityQueue<Mail> queue = new PriorityQueue<>();

    // 邮箱所属的主线程
    private final Thread mailboxThread;

    // 邮箱状态
    private volatile State state = State.OPEN;

    public TaskMailboxImpl(Thread mailboxThread) {
        this.mailboxThread = mailboxThread;
    }

    @Override
    public boolean hasMail() {
        lock.lock();
        try {
            return !queue.isEmpty();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Optional<Mail> tryTake(int priority) {
        checkIsMailboxThread(); // 只有主线程能取信
        lock.lock();
        try {
            Mail head = queue.peek();
            if (head == null) {
                return Optional.empty();
            }
            // 在完整 Flink 实现中，这里可以判断 head.priority 是否满足要求
            // 简化版中，PriorityQueue 保证了 head 永远是优先级最高的
            return Optional.ofNullable(queue.poll());
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Mail take(int priority) throws InterruptedException {
        checkIsMailboxThread(); // 只有主线程能取信
        lock.lock();
        try {
            // 循环等待模式
            while (queue.isEmpty()) {
                if (state == State.CLOSED) {
                    throw new IllegalStateException("邮箱已关闭");
                }
                // 阻塞，释放锁，等待被 put() 唤醒
                notEmpty.await();
            }
            // PriorityQueue 保证 poll 出来的是优先级最高的
            return queue.poll();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void put(Mail mail) {
        lock.lock();
        try {
            if (state == State.CLOSED) {
                log.warn("邮箱已关闭，正在丢弃邮件：" + mail);
                return;
            }
            // 修改点: 使用 offer (PriorityQueue 方法)
            queue.offer(mail);
            // 唤醒睡在 take() 里的主线程
            notEmpty.signal();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() {
        lock.lock();
        try {
            state = State.CLOSED;
            // 唤醒所有等待的线程
            notEmpty.signalAll();
            queue.clear();
        } finally {
            lock.unlock();
        }
    }

    private void checkIsMailboxThread() {
        if (Thread.currentThread() != mailboxThread) {
            throw new IllegalStateException(
                    "非法线程访问。预期: " + mailboxThread.getName() +
                            ", 实际: " + Thread.currentThread().getName());
        }
    }
}
