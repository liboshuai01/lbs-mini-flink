package cn.liboshuai.flink.runtime.tasks.mailbox;

import cn.liboshuai.flink.util.ThrowingRunnable;

public class MailboxExecutorImpl implements MailboxExecutor {

    private final TaskMailbox mailbox;
    private final int priority;

    public MailboxExecutorImpl(TaskMailbox mailbox, int priority) {
        this.mailbox = mailbox;
        this.priority = priority;
    }

    @Override
    public void execute(ThrowingRunnable<? extends Exception> command, String description) {
        // 包装成 Mail 并扔进邮箱
        mailbox.put(new Mail(command, priority, description));
    }

}
