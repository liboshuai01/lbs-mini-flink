package cn.liboshuai.flink.runtime.tasks.mailbox;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;

/**
 * 对应 Flink 源码中的 StreamOneInputProcessor。
 * 修改点：
 * 1. 构造函数接收 TaskMailbox。
 * 2. 在批处理循环中，通过 mailbox.hasMail() 判断是否需要让步。
 */
@Slf4j
public class StreamInputProcessor implements MailboxDefaultAction {

    private final MiniInputGate inputGate;
    private final DataOutput output;
    private final TaskMailbox mailbox; // [新增] 持有邮箱引用，用于检查让步

    // 批处理大小
    private static final int BATCH_SIZE = 10;

    public interface DataOutput {
        void processRecord(String record);
    }

    public StreamInputProcessor(MiniInputGate inputGate, DataOutput output, TaskMailbox mailbox) {
        this.inputGate = inputGate;
        this.output = output;
        this.mailbox = mailbox;
    }

    @Override
    public void runDefaultAction(Controller controller) {
        int processedCount = 0;

        // [核心逻辑]
        // 循环继续的条件：
        // 1. 还没处理完一个批次 (processedCount < BATCH_SIZE)
        // 2. 邮箱里没有新邮件 (!mailbox.hasMail()) -> 这就是让步逻辑
        //    只要有任何邮件（Checkpoint, Timer 等），hasMail() 返回 true，循环终止，控制权交还给 MailboxProcessor
        while (processedCount < BATCH_SIZE && !mailbox.hasMail()) {

            String record = inputGate.pollNext();

            if (record == null) {
                processEmptyInput(controller);
                return;
            }

            output.processRecord(record);
            processedCount++;
        }

        // 调试日志：如果是因为有邮件而退出的
        if (processedCount < BATCH_SIZE && mailbox.hasMail()) {
            log.info("检测到邮箱有信，StreamInputProcessor 主动让步 (Yield)。");
        }
    }

    private void processEmptyInput(Controller controller) {
        CompletableFuture<Void> availableFuture = inputGate.getAvailableFuture();

        if (availableFuture.isDone()) {
            return;
        }

        controller.suspendDefaultAction();

        availableFuture.thenRun(() -> {
            ((MailboxProcessor) controller).resumeDefaultAction();
        });
    }
}