package cn.liboshuai.flink.runtime.tasks.mailbox;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.TimeUnit;

/**
 * 这是一个具体的业务 Task。
 * 目标：演示在不加锁的情况下，处理数据和 Checkpoint 读取状态的安全性。
 */
@Slf4j
public class CounterStreamTask extends StreamTask implements StreamInputProcessor.DataOutput {

    private final StreamInputProcessor inputProcessor;

    // === 核心状态 ===
    // 在多线程模型中，这里必须 volatile 甚至 AtomicLong，或者加 synchronized
    // 但在 Mailbox 模型中，它只是一个普通的 long，因为只有主线程能访问它！
    private long recordCount = 0;

    public CounterStreamTask(MiniInputGate inputGate) {
        super();
        // 初始化 Processor，将自己作为 Output 传入
        this.inputProcessor = new StreamInputProcessor(inputGate, this);
    }

    @Override
    public void runDefaultAction(Controller controller) {
        // 委托给 Processor 处理输入
        inputProcessor.runDefaultAction(controller);
    }

    @Override
    public void processRecord(String record) {
        // [主线程] 正在处理数据
        this.recordCount++;

        // 模拟一点计算耗时
        if (recordCount % 10 == 0) {
            log.info("Task 处理进度: {} 条", recordCount);
        }
    }

    /**
     * 执行 Checkpoint 的逻辑。
     * 这个方法会被封装成一个 Mail，由 CheckpointCoordinator 扔进邮箱。
     * 因为是从邮箱取出来执行的，所以它一定是在 [主线程] 运行。
     */
    public void performCheckpoint(long checkpointId) {
        // [主线程] 正在执行 Checkpoint
        // 此时 processRecord 绝对不会运行，因为是串行的！

        log.info(" >>> [Checkpoint Starting] ID: {}, 当前状态值: {}", checkpointId, recordCount);

        // 模拟状态快照耗时
        try { Thread.sleep(50); } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        log.info(" <<< [Checkpoint Finished] ID: {} 完成", checkpointId);
    }
}
