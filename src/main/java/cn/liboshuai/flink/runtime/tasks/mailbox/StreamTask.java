package cn.liboshuai.flink.runtime.tasks.mailbox;

import lombok.extern.slf4j.Slf4j;

/**
 * 任务基类。
 * 核心修改：明确区分 Control Flow (优先级0) 和 Data Flow (优先级1) 的 Executor 配置。
 */
@Slf4j
public abstract class StreamTask implements MailboxDefaultAction {

    protected final TaskMailbox mailbox;
    protected final MailboxProcessor mailboxProcessor;
    protected final MailboxExecutor mainMailboxExecutor;

    public StreamTask() {
        Thread currentThread = Thread.currentThread();
        this.mailbox = new TaskMailboxImpl(currentThread);
        this.mailboxProcessor = new MailboxProcessor(this, mailbox);
        // 主执行器 (用于 task 内部自提交) 跟随 Processor 的默认优先级 (1)
        this.mainMailboxExecutor = mailboxProcessor.getMainExecutor();
    }

    public final void invoke() throws Exception {
        log.info("[StreamTask] 任务已启动。");
        try {
            mailboxProcessor.runMailboxLoop();
        } catch (Exception e) {
            log.error("[StreamTask] 异常：" + e.getMessage());
            throw e;
        } finally {
            close();
        }
    }

    private void close() {
        log.info("[StreamTask] 结束。");
        mailbox.close();
    }

    /**
     * 获取控制平面执行器。
     * [关键] 强制使用 MailboxProcessor.MIN_PRIORITY (0)。
     * 只有这样，CheckpointScheduler 提交的任务才会拥有 priority=0，
     * 从而在 MailboxProcessor 的 "阶段 1" 中被优先抢占执行。
     */
    public MailboxExecutor getControlMailboxExecutor() {
        return new MailboxExecutorImpl(mailbox, MailboxProcessor.MIN_PRIORITY);
    }

    @Override
    public abstract void runDefaultAction(Controller controller) throws Exception;

    public abstract void performCheckpoint(long checkpointId);
}