package cn.liboshuai.flink.runtime.tasks.mailbox;

public interface MailboxDefaultAction {

    void runDefaultAction(Controller controller) throws Exception;

    interface Controller {
        void suspendDefaultAction();
    }
}
