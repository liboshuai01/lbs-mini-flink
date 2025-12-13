package cn.liboshuai.flink.runtime.tasks.mailbox;

import cn.liboshuai.flink.util.ThrowingRunnable;
import lombok.Getter;

public class Mail {
    private final ThrowingRunnable<? extends Exception> runnable;
    @Getter
    private final int priority;
    private final String description;

    public Mail(ThrowingRunnable<? extends Exception> runnable, int priority, String description) {
        this.runnable = runnable;
        this.priority = priority;
        this.description = description;
    }

    void run() throws Exception {
        runnable.run();
    }

    @Override
    public String toString() {
        return description;
    }
}
