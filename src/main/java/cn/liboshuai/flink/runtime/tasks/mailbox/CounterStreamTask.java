package cn.liboshuai.flink.runtime.tasks.mailbox;

import lombok.extern.slf4j.Slf4j;

/**
 * 任务实现类。
 * 修改点：在创建 StreamInputProcessor 时将自己的 mailbox 传进去。
 */
@Slf4j
public class CounterStreamTask extends StreamTask implements StreamInputProcessor.DataOutput {

    private final StreamInputProcessor inputProcessor;
    private long recordCount = 0;

    public CounterStreamTask(MiniInputGate inputGate) {
        super();
        // [修改] 将 this.mailbox 传给 Processor，使其具备让步检测能力
        this.inputProcessor = new StreamInputProcessor(inputGate, this, this.mailbox);
    }

    @Override
    public void runDefaultAction(Controller controller) {
        inputProcessor.runDefaultAction(controller);
    }

    @Override
    public void processRecord(String record) {
        this.recordCount++;
        if (recordCount % 10 == 0) {
            log.info("Task 处理进度: {} 条", recordCount);
        }
    }

    @Override
    public void performCheckpoint(long checkpointId) {
        log.info(" >>> [Checkpoint Starting] ID: {}, 当前状态值: {}", checkpointId, recordCount);
        try { Thread.sleep(50); } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        log.info(" <<< [Checkpoint Finished] ID: {} 完成", checkpointId);
    }
}