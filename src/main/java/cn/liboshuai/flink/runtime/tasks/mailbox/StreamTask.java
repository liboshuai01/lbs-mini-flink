package cn.liboshuai.flink.runtime.tasks.mailbox;

import lombok.extern.slf4j.Slf4j;

/**
 * 任务基类。
 * 修改点：getControlMailboxExecutor 使用最高优先级 (MIN_PRIORITY)。
 */
@Slf4j
public abstract class StreamTask implements MailboxDefaultAction {

    protected final TaskMailbox mailbox;
    protected final MailboxProcessor mailboxProcessor;
    protected final MailboxExecutor mainMailboxExecutor;

    public StreamTask() {
        // 1. 获取当前线程作为主线程
        Thread currentThread = Thread.currentThread();

        // 2. 初始化邮箱 (使用新的 TaskMailboxImpl)
        this.mailbox = new TaskMailboxImpl(currentThread);

        // 3. 初始化处理器
        this.mailboxProcessor = new MailboxProcessor(this, mailbox);

        // 4. 获取主线程 Executor
        this.mainMailboxExecutor = mailboxProcessor.getMainExecutor();
    }

    /**
     * 任务执行的主入口
     */
    public final void invoke() throws Exception {
        log.info("[StreamTask] 任务已启动。正在进入邮箱循环...");
        try {
            // 启动主循环
            mailboxProcessor.runMailboxLoop();
        } catch (Exception e) {
            log.error("[StreamTask] 主循环异常：" + e.getMessage());
            throw e;
        } finally {
            close();
        }
    }

    private void close() {
        log.info("[StreamTask] 任务已完成/结束。");
        mailbox.close();
    }

    /**
     * 获取用于提交 Checkpoint 等控制消息的 Executor (高优先级)
     * 修改点：使用 MailboxProcessor.MIN_PRIORITY (0)
     */
    public MailboxExecutor getControlMailboxExecutor() {
        return new MailboxExecutorImpl(mailbox, MailboxProcessor.MIN_PRIORITY);
    }

    // 子类实现具体的处理逻辑
    @Override
    public abstract void runDefaultAction(Controller controller) throws Exception;

    // 执行 Checkpoint 行为, 由子类实现
    public abstract void performCheckpoint(long checkpointId);
}
