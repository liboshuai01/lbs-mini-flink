package cn.liboshuai.flink.runtime.tasks.mailbox;

import cn.liboshuai.flink.util.ThrowingRunnable;

public interface MailboxExecutor {
    void execute(ThrowingRunnable<? extends Exception> command, String description);

    void yield() throws InterruptedException, Exception;
}
