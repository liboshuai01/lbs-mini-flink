package cn.liboshuai.flink.runtime.tasks.mailbox;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class StreamTask implements MailboxDefaultAction {

    protected final TaskMailbox mailbox;
    protected final MailboxProcessor mailboxProcessor;
    protected final MailboxExecutor mainMailboxExecutor;

    public StreamTask() {
        // 1. 获取当前线程作为主线程
        Thread currentThread = Thread.currentThread();

        // 2. 初始化邮箱
        this.mailbox = new TaskMailboxImpl(currentThread);

        // 3. 初始化处理器 (将 this 作为 DefaultAction 传入)
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
            // 启动主循环，直到任务取消或完成
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
     */
    public MailboxExecutor getControlMailboxExecutor() {
        // 假设优先级 10 用于控制消息
        return new MailboxExecutorImpl(mailbox, 10);
    }

    // 子类实现具体的处理逻辑
    @Override
    public abstract void runDefaultAction(Controller controller) throws Exception;
}
