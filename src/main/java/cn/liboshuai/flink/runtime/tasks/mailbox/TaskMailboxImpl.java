package cn.liboshuai.flink.runtime.tasks.mailbox;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Optional;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

@Slf4j
public class TaskMailboxImpl implements TaskMailbox {

    private final Deque<Mail> queue = new ArrayDeque<>();

    private final ReentrantLock lock = new ReentrantLock();

    private final Condition notEmpty = lock.newCondition();

    private final Thread mailboxThread;

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
        checkIsMailboxThread();
        lock.lock();
        try {
            return Optional.ofNullable(queue.pollFirst());
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Mail take(int priority) throws InterruptedException {
        checkIsMailboxThread();
        lock.lock();
        try {
            while (queue.isEmpty()) {
                if (state == State.CLOSED) {
                    throw new IllegalStateException("Mailbox 已经关闭了");
                }
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
                log.warn("Mailbox 已经关闭了, 此 Mail 被丢弃了: {}", mail);
                return;
            }
            queue.addLast(mail);
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
            notEmpty.signalAll();
            queue.clear();
        } finally {
            lock.unlock();
        }
    }

    void checkIsMailboxThread() {
        if (mailboxThread != Thread.currentThread()) {
            throw new IllegalStateException(
                    "非法线程访问." +
                            "预期: " + mailboxThread.getName() +
                            "实际: " + Thread.currentThread().getName()
            );
        }
    }
}
