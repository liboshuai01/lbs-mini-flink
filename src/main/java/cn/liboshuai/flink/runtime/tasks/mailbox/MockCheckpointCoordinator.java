package cn.liboshuai.flink.runtime.tasks.mailbox;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.TimeUnit;

@Slf4j
public class MockCheckpointCoordinator extends Thread{

    private final MailboxExecutor mailboxExecutor;

    private volatile boolean isRunning = true;

    public MockCheckpointCoordinator(MailboxExecutor mailboxExecutor) {
        super("Checkpoint-Coordinator");
        this.mailboxExecutor = mailboxExecutor;
    }

    @Override
    public void run() {
        long checkpointId = 1;
        while (isRunning) {
            try {
                TimeUnit.MILLISECONDS.sleep(1500);
                long currentId = ++checkpointId;
                log.info("[JM] 触发 Checkpoint {}", currentId);
                mailboxExecutor.execute(() -> {
                    log.info(">>> [MainThread] 正在执行 Checkpoint {} 中", currentId);
                    try {
                        TimeUnit.MILLISECONDS.sleep(50);
                    } catch (InterruptedException e) {}
                }, "Checkpoint-" + currentId);
            } catch (InterruptedException e) {
                break;
            }
        }
    }

    public void shutdown() {
        isRunning = false;
        this.interrupt();
    }
}
