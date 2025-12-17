package cn.liboshuai.flink.runtime.tasks.mailbox;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.Optional;

@Slf4j
public class MailboxProcessor implements MailboxDefaultAction.Controller {

    public static final int MIN_PRIORITY = 0;
    public static final int DEFAULT_PRIORITY = 1;

    private final MailboxDefaultAction defaultAction;
    private final TaskMailbox mailbox;

    @Getter
    private final MailboxExecutor mainExecutor;

    private boolean isDefaultActionAvailable = true;

    public MailboxProcessor(MailboxDefaultAction defaultAction, TaskMailbox mailbox) {
        this.defaultAction = defaultAction;
        this.mailbox = mailbox;
        this.mainExecutor = new MailboxExecutorImpl(mailbox, DEFAULT_PRIORITY);
    }

    public void runMailboxLoop() throws Exception {
        while (true) {
            // 阶段 1: 处理所有待处理的邮件 (Checkpoint, Timers 等)
            // 只要有邮件，就一直处理，直到邮箱为空或只剩下优先级不够的邮件
            while (processMail(mailbox, MIN_PRIORITY)) {
                // loop
            }

            // 阶段 2: 执行默认动作 (数据处理)
            if (isDefaultActionAvailable) {
                // StreamInputProcessor 在内部进行批处理时，会主动检查 mailbox.hasMail()
                defaultAction.runDefaultAction(this);
            } else {
                // 阶段 3: 挂起，阻塞等待新邮件
                Mail mail = mailbox.take(DEFAULT_PRIORITY);
                mail.run();
            }
        }
    }

    private boolean processMail(TaskMailbox mailbox, int priority) throws Exception {
        Optional<Mail> mail = mailbox.tryTake(priority);
        if (mail.isPresent()) {
            mail.get().run();
            return true;
        }
        return false;
    }

    // --- Controller 接口实现 ---

    @Override
    public void suspendDefaultAction() {
        this.isDefaultActionAvailable = false;
    }

    public void resumeDefaultAction() {
        mailbox.put(new Mail(
                () -> this.isDefaultActionAvailable = true,
                MIN_PRIORITY,
                "Resume Default Action"
        ));
    }
}