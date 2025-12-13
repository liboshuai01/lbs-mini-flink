package cn.liboshuai.flink.runtime.tasks.mailbox;

import java.util.Random;
import java.util.concurrent.TimeUnit;

public class MockNettyThread extends Thread{

    private final MockInputGate inputGate;

    private volatile boolean isRunning = true;

    public MockNettyThread(MockInputGate inputGate) {
        super("Netty-IO-Thread");
        this.inputGate = inputGate;
    }

    @Override
    public void run() {
        int seq = 0;
        Random random = new Random();
        while (isRunning) {
            try {
                int sleepTime = random.nextInt(10) > 7 ? 2000 : 100;
                TimeUnit.MILLISECONDS.sleep(sleepTime);
                String data = "Record-" + (++seq);
                inputGate.push(data);
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
