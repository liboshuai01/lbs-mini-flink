package cn.liboshuai.flink.runtime.tasks.mailbox;

import java.util.Optional;

public interface TaskMailbox {

    boolean hasMail();

    Optional<Mail> tryTake(int priority);

    Mail take(int priority) throws InterruptedException;

    void put(Mail mail);

    void close();

    enum State {
        OPEN,
        QUIESCED,
        CLOSED
    }

}
