package cn.liboshuai.flink.runtime.tasks.mailbox;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MiniFlinkComplexApp {
    public static void main(String[] args) {
        log.info("================== 启动 Mini-Flink ==================");

        MockInputGate mockInputGate = new MockInputGate();

        StreamTask sourceStreamTask = new SourceStreamTask(mockInputGate);

        MockNettyThread mockNettyThread = new MockNettyThread(mockInputGate);
        mockNettyThread.start();

        MockCheckpointCoordinator mockCheckpointCoordinator = new MockCheckpointCoordinator(sourceStreamTask.getControlMailboxExecutor());
        mockCheckpointCoordinator.start();

        try {
            sourceStreamTask.invoke();
        } catch (Exception e) {
            log.error("", e);
        } finally {
            mockNettyThread.shutdown();
            mockCheckpointCoordinator.shutdown();
        }
    }
}
