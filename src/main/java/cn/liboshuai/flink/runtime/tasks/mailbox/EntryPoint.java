package cn.liboshuai.flink.runtime.tasks.mailbox;

import lombok.extern.slf4j.Slf4j;

/**
 * 新版 MiniFlink 启动类。
 * 展示了 Mailbox 模型如何协调 IO 线程、控制线程和主计算线程。
 */
@Slf4j
public class EntryPoint {

    public static void main(String[] args) {
        log.info("=== Flink Mailbox 模型深度模拟启动 ===");

        // 1. 构建组件
        MiniInputGate inputGate = new MiniInputGate();

        // Task 创建时会初始化自己的 Mailbox 和 Processor
        // 注意：Task 必须在主线程（即这里的 main 线程）运行逻辑
        CounterStreamTask task = new CounterStreamTask(inputGate);

        // 2. 启动外部线程
        NettyDataProducer netty = new NettyDataProducer(inputGate);
        netty.start();

        CheckpointScheduler cpCoordinator = new CheckpointScheduler(task);
        cpCoordinator.start();

        // 3. 启动 Task 主循环 (阻塞当前 Main 线程)
        try {
            // 这行代码之后，Main 线程变成了 Task 线程
            // 它会在以下状态切换：
            // - 处理 Mail (Checkpoint)
            // - 处理 DefaultAction (消费 InputGate)
            // - Suspend (等待 InputGate 的 Future)
            task.invoke();
        } catch (Exception e) {
            log.error("Task 崩溃", e);
        } finally {
            netty.shutdown();
            cpCoordinator.shutdown();
        }
    }
}