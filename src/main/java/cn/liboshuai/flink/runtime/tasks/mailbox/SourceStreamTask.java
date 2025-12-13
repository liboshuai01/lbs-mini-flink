package cn.liboshuai.flink.runtime.tasks.mailbox;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.TimeUnit;

@Slf4j
public class SourceStreamTask extends StreamTask {

    private final MockInputGate inputGate;

    public SourceStreamTask(MockInputGate inputGate) {
        super();
        this.inputGate = inputGate;
    }

    @Override
    public void runDefaultAction(Controller controller) throws Exception {
        String data = inputGate.poll();
        if (data != null) {
            processData(data);
        } else {
            log.info("    [Task] 输入数据为空. 暂停数据处理...");
            controller.suspendDefaultAction();
            inputGate.registerAvailabilityListener(() -> {
                log.info("    [Netty-Task] 新数据已经送达! 恢复数据处理...");
                mailboxProcessor.resumeDefaultAction();
            });
        }
    }

    void processData(String data) {
        log.info("    [Task] 数据处理中: {}", data);
        try {
            TimeUnit.MILLISECONDS.sleep(200);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
