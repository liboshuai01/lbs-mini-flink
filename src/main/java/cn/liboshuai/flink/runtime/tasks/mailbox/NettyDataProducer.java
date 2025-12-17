package cn.liboshuai.flink.runtime.tasks.mailbox;

import java.util.Random;
import java.util.concurrent.TimeUnit;

public class NettyDataProducer extends Thread {
    private final MiniInputGate inputGate;
    private volatile boolean running = true;

    public NettyDataProducer(MiniInputGate inputGate) {
        super("Netty-Thread");
        this.inputGate = inputGate;
    }

    @Override
    public void run() {
        Random random = new Random();
        int seq = 0;
        while (running) {
            try {
                // 模拟网络抖动：大部分时候很快，偶尔卡顿
                // 这能测试 Task 在"忙碌"和"挂起"状态之间的切换
                int sleep = random.nextInt(100) < 5 ? 500 : 10;
                TimeUnit.MILLISECONDS.sleep(sleep);

                String data = "Record-" + (++seq);
                inputGate.pushData(data);

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
