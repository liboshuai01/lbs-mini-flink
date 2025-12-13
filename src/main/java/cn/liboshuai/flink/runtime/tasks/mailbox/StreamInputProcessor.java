package cn.liboshuai.flink.runtime.tasks.mailbox;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;

/**
 * 对应 Flink 源码中的 StreamOneInputProcessor。
 * 它是 MailboxDefaultAction 的具体实现者。
 * 它的职责是：不断拉取数据，如果没数据了，就告诉 Mailbox "我没活了，挂起我"。
 */
@Slf4j
public class StreamInputProcessor implements MailboxDefaultAction {

    private final MiniInputGate inputGate;
    private final DataOutput output;

    public interface DataOutput {
        void processRecord(String record);
    }

    public StreamInputProcessor(MiniInputGate inputGate, DataOutput output) {
        this.inputGate = inputGate;
        this.output = output;
    }

    @Override
    public void runDefaultAction(Controller controller) {
        // 1. 尝试从 InputGate 拿数据
        String record = inputGate.pollNext();

        if (record != null) {
            // A. 有数据，直接处理
            // 注意：这里是在主线程执行，非常安全
            output.processRecord(record);
        } else {
            // B. 没数据了 (InputGate 空)
//            log.info("[Task] 输入数据为空. 暂停数据处理...");

            // 1. 获取 InputGate 的"可用性凭证" (Future)
            CompletableFuture<Void> availableFuture = inputGate.getAvailableFuture();

            if (availableFuture.isDone()) {
                // 极低概率：刚 poll 完是空，但这微秒间 Netty 又塞了一个并在 pollNext 内部 complete 了 future
                // 那么直接 return，下一轮循环再 poll 即可
                return;
            }

            // 2. 告诉 MailboxProcessor：暂停默认动作 (Suspend)
            // 此时主线程会停止疯狂空转，进入 mailbox.take() 的阻塞睡眠状态，或者处理其他 Mail
            controller.suspendDefaultAction();

            // 3. 核心桥梁：当 Future 完成时（即 Netty 推数据了），向 Mailbox 发送一个"恢复信号"
            // thenRun 是在 complete 这个 Future 的线程（即 Netty 线程）中执行的
            availableFuture.thenRun(() -> {
                // 注意：这里是在 Netty 线程运行，所以要跨线程调用 resume
                // 这会往 Mailbox 塞一个高优先级的 "Resume Mail"
//                log.debug("[Netty->Task] 数据到达，触发 Resume");
                ((MailboxProcessor) controller).resumeDefaultAction();
            });
        }
    }
}