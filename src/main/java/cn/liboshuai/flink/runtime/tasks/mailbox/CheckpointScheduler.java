package cn.liboshuai.flink.runtime.tasks.mailbox;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.TimeUnit;

@Slf4j
public class CheckpointScheduler extends Thread {

    private final MailboxExecutor taskMailboxExecutor;
    private final StreamTask task;
    private volatile boolean running = true;

    public CheckpointScheduler(CounterStreamTask task) {
        super("Checkpoint-Timer");
        this.task = task;
        // 获取高优先级的执行器 (Checkpoint 优先级 > 数据处理)
        this.taskMailboxExecutor = task.getControlMailboxExecutor();
    }

    @Override
    public void run() {
        long checkpointId = 0;
        while (running) {
            try {
                TimeUnit.MILLISECONDS.sleep(2000); // 每2秒触发一次
                long id = ++checkpointId;

                log.info("[JM] 触发 Checkpoint {}", id);

                // === 关键点 ===
                // 我们不在这里调用 task.performCheckpoint()，因为那会导致线程不安全。
                // 我们创建一个 Mail (Lambda)，扔给 Task 线程自己去跑。
                taskMailboxExecutor.execute(
                        () -> task.performCheckpoint(id),
                        "Checkpoint-" + id
                );

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public void shutdown() {
        running = false;
        this.interrupt();
    }
}
