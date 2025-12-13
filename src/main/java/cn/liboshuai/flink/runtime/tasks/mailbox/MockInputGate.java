package cn.liboshuai.flink.runtime.tasks.mailbox;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.locks.ReentrantLock;

public class MockInputGate {

    private final Deque<String> queue = new ArrayDeque<>();

    private final ReentrantLock lock = new ReentrantLock();

    private Runnable availabilityListener;

    public void push(String data) {
        lock.lock();
        try {
            queue.addLast(data);
            if (availabilityListener != null) {
                Runnable listener = this.availabilityListener;
                availabilityListener = null;
                listener.run();
            }
        } finally {
            lock.unlock();
        }
    }

    public String poll() {
        lock.lock();
        try {
            return queue.pollFirst();
        } finally {
            lock.unlock();
        }
    }

    public void registerAvailabilityListener(Runnable listener) {
        lock.lock();
        try {
            if (!queue.isEmpty()) {
                listener.run();
            } else {
                this.availabilityListener = listener;
            }
        } finally {
            lock.unlock();
        }
    }

}
