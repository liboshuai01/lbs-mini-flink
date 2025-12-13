package cn.liboshuai.flink.runtime.tasks.mailbox;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class StreamTask implements MailboxDefaultAction {

    protected final TaskMailbox mailbox;
    protected final MailboxProcessor mailboxProcessor;
    protected final MailboxExecutor mailboxExecutor;

    protected volatile boolean isRunning = true;

    protected StreamTask() {
        this.mailbox = new TaskMailboxImpl(Thread.currentThread());
        this.mailboxProcessor = new MailboxProcessor(this, mailbox);
        this.mailboxExecutor = mailboxProcessor.getMailboxExecutor();
    }

    public final void invoke() throws Exception {
        log.info("[StreamTask] Task 正在启动, 进入 Mailbox 主循环...");
        try {
            mailboxProcessor.runMailboxLoop();
        } catch (Exception e) {
            log.error("[StreamTask] 执行 Mailbox 主循环时出现异常: {}", e.getMessage());
            throw e;
        } finally {
            close();
        }
    }

    public void close() {
        isRunning = false;
        mailbox.close();
    }

    public MailboxExecutor getControlMailboxExecutor() {
        return new MailboxExecutorImpl(mailbox, 10);
    }
}
