package cn.liboshuai.flink.runtime.tasks.mailbox;

import lombok.Getter;

import java.util.Optional;

public class MailboxProcessor implements MailboxDefaultAction.Controller{

    private final MailboxDefaultAction defaultAction;
    private final TaskMailbox mailbox;
    @Getter
    private final MailboxExecutor mailboxExecutor;

    private boolean isDefaultActionAvailable = true;

    public MailboxProcessor(MailboxDefaultAction defaultAction, TaskMailbox mailbox) {
        this.defaultAction = defaultAction;
        this.mailbox = mailbox;
        mailboxExecutor = new MailboxExecutorImpl(mailbox, 0);
    }

    public void runMailboxLoop() throws Exception {
        while (true) {
            while (mailbox.hasMail()) {
                Optional<Mail> mailOptional = mailbox.tryTake(0);
                if (mailOptional.isPresent()) {
                    mailOptional.get().run();
                }
            }
            if (isDefaultActionAvailable) {
                defaultAction.runDefaultAction(this);
            } else {
                Mail mail = mailbox.take(0);
                mail.run();
            }
        }
    }

    @Override
    public void suspendDefaultAction() {
        this.isDefaultActionAvailable = false;
    }

    public void resumeDefaultAction() {
        mailboxExecutor.execute(() -> {
            this.isDefaultActionAvailable = true;
        }, "恢复数据处理操作");
    }
}
