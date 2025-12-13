package cn.liboshuai.flink.runtime.tasks.mailbox;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Optional;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

@Slf4j
public class TaskMailboxImpl implements TaskMailbox {

    // 核心锁
    private final ReentrantLock lock = new ReentrantLock();

    // 条件变量：队列不为空
    private final Condition notEmpty = lock.newCondition();

    // 内部队列，使用非线程安全的 ArrayDeque 即可，因为我们有 lock 保护
    private final Deque<Mail> queue = new ArrayDeque<>();

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
            return Optional.ofNullable(queue.pollFirst());
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Mail take(int priority) throws InterruptedException {
        checkIsMailboxThread(); // 只有主线程能取信
        lock.lock();
        try {
            // 循环等待模式 (Standard Monitor Pattern)
            while (queue.isEmpty()) {
                if (state == State.CLOSED) {
                    throw new IllegalStateException("邮箱已关闭");
                }
                // 阻塞，释放锁，等待被 put() 唤醒
                notEmpty.await();
            }
            return queue.pollFirst();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void put(Mail mail) {
        lock.lock();
        try {
            if (state == State.CLOSED) {
                // 如果关闭了，静默丢弃或抛异常，这里我们打印日志
                log.warn("邮箱已关闭，正在丢弃邮件：" + mail);
                return;
            }
            // 入队
            queue.addLast(mail);
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
            // 唤醒所有等待的线程，让它们抛出异常或退出
            notEmpty.signalAll();
            // 清空剩余邮件 (在真实 Flink 中通常会把剩余的执行完或做清理)
            queue.clear();
        } finally {
            lock.unlock();
        }
    }

    /**
     * 关键防御性编程：确保单线程模型不被破坏
     */
    private void checkIsMailboxThread() {
        if (Thread.currentThread() != mailboxThread) {
            throw new IllegalStateException(
                    "非法线程访问。" +
                            "预期: " + mailboxThread.getName() +
                            ", 实际: " + Thread.currentThread().getName());
        }
    }
}
