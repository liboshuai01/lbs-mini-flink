package cn.liboshuai.flink.runtime.tasks.mailbox;

import lombok.Getter;

import java.util.Optional;

/**
 * 邮箱处理器，负责主循环逻辑。
 * 修改点：定义了优先级常量，并调整了 mainExecutor 的默认优先级。
 */
public class MailboxProcessor implements MailboxDefaultAction.Controller {

    // 修改点: 定义明确的优先级常量
    // 0: 最高优先级 (System/Checkpoint/Control)
    // 1: 默认优先级 (Data Processing)
    public static final int MIN_PRIORITY = 0;
    public static final int DEFAULT_PRIORITY = 1;

    private final MailboxDefaultAction defaultAction;
    private final TaskMailbox mailbox;

    @Getter
    private final MailboxExecutor mainExecutor;

    // 标记默认动作是否可用
    private boolean isDefaultActionAvailable = true;

    public MailboxProcessor(MailboxDefaultAction defaultAction, TaskMailbox mailbox) {
        this.defaultAction = defaultAction;
        this.mailbox = mailbox;
        // 修改点: 主线程处理数据的 Executor 优先级设为 DEFAULT_PRIORITY (1)
        // 这样 Checkpoint (优先级 0) 就可以插队执行
        this.mainExecutor = new MailboxExecutorImpl(mailbox, DEFAULT_PRIORITY);
    }

    /**
     * 启动主循环 (The Main Loop)
     */
    public void runMailboxLoop() throws Exception {

        while (true) {
            // 阶段 1: 处理所有积压的邮件 (系统事件优先)
            // PriorityQueue 保证了 poll() 总是先取出优先级 0 的邮件，再取出优先级 1 的
            while (mailbox.hasMail()) {
                // 使用 MIN_PRIORITY 表示我们愿意处理任何优先级 >= 0 的邮件
                Optional<Mail> mail = mailbox.tryTake(MIN_PRIORITY);
                if (mail.isPresent()) {
                    mail.get().run();
                }
            }

            // 阶段 2: 执行默认动作 (数据处理)
            if (isDefaultActionAvailable) {
                // 执行一小步数据处理
                defaultAction.runDefaultAction(this);
            } else {
                // 阶段 3: 如果没事干 (DefaultAction 被挂起)，就阻塞等待新邮件
                Mail mail = mailbox.take(MIN_PRIORITY);
                mail.run();
            }
        }
    }

    // --- Controller 接口实现 ---

    @Override
    public void suspendDefaultAction() {
        this.isDefaultActionAvailable = false;
    }

    public void resumeDefaultAction() {
        // 恢复默认动作的信号。
        // 可以使用高优先级，也可以使用默认优先级。
        mainExecutor.execute(() -> this.isDefaultActionAvailable = true,
                "Resume Default Action");
    }
}
