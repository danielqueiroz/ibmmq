package com.ibmmq;

import com.ibm.mq.ese.nls.AmsErrorMessageInserts;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;

public class ConsumerWorker implements Runnable {

    private final BlockingQueue<String> receivedMessageQueue;
    private int consumed;


    public ConsumerWorker(BlockingQueue<String> receivedMessageQueue) {
        this.receivedMessageQueue = receivedMessageQueue;
    }

    @Override
    public void run() {
        while(consumed < 5) {
            if (!receivedMessageQueue.isEmpty()) {
                List<String> msgs = new ArrayList<>();
                receivedMessageQueue.drainTo(msgs);
                msgs.forEach(System.out::println);
                consumed+=msgs.size();
            }
        }
    }
}
