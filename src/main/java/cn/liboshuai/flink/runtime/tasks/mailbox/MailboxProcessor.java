package cn.liboshuai.flink.runtime.tasks.mailbox;

import lombok.Getter;

import java.util.Optional;

public class MailboxProcessor implements MailboxDefaultAction.Controller {

    private final MailboxDefaultAction defaultAction;
    private final TaskMailbox mailbox;
    @Getter
    private final MailboxExecutor mainExecutor;

    // 标记默认动作是否可用（是否可以执行 processInput）
    private boolean isDefaultActionAvailable = true;

    public MailboxProcessor(MailboxDefaultAction defaultAction, TaskMailbox mailbox) {
        this.defaultAction = defaultAction;
        this.mailbox = mailbox;
        // 创建一个绑定了最低优先级的 Executor 给主线程自己用
        this.mainExecutor = new MailboxExecutorImpl(mailbox, 0);
    }

    /**
     * 启动主循环 (The Main Loop)
     */
    public void runMailboxLoop() throws Exception {

        while (true) {
            // 阶段 1: 处理所有积压的邮件 (系统事件优先)
            // 我们使用 tryTake 非阻塞地把邮箱清空
            while (mailbox.hasMail()) {
                Optional<Mail> mail = mailbox.tryTake(0);
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
                // take() 会让线程睡着，直到有外部线程 put()
                Mail mail = mailbox.take(0);
                mail.run();
            }
        }
    }

    // --- Controller 接口实现 ---

    @Override
    public void suspendDefaultAction() {
        // 收到暂停请求
        this.isDefaultActionAvailable = false;
    }

    /**
     * 这是一个用于恢复默认动作的辅助方法。
     * 外部线程（如 Netty）调用此方法来“唤醒”主循环。
     */
    public void resumeDefaultAction() {
        // 我们通过发一个特殊的“空邮件”或者“恢复邮件”来唤醒阻塞在 take() 的主线程
        // 同时修改标记位
        mainExecutor.execute(() -> this.isDefaultActionAvailable = true,
                "Resume Default Action");
    }
}
