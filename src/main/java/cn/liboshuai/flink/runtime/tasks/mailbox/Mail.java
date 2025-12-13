package cn.liboshuai.flink.runtime.tasks.mailbox;

import cn.liboshuai.flink.util.ThrowingRunnable;
import lombok.Getter;

public class Mail {

    // 真正的业务逻辑 (例如：执行 Checkpoint，或者处理一条数据)
    private final ThrowingRunnable<? extends Exception> runnable;

    // 优先级 (数字越小优先级越高，虽在简易版中我们暂按 FIFO 处理，但保留字段)
    @Getter
    private final int priority;

    // 描述信息，用于调试 (例如 "Checkpoint 15")
    private final String description;

    public Mail(ThrowingRunnable<? extends Exception> runnable, int priority, String description) {
        this.runnable = runnable;
        this.priority = priority;
        this.description = description;
    }

    /**
     * 执行邮件中的逻辑
     */
    public void run() throws Exception {
        runnable.run();
    }

    @Override
    public String toString() {
        return description;
    }
}
