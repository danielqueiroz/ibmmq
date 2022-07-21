package com.ibmmq;

import com.ibm.msg.client.jms.JmsConnectionFactory;
import com.ibm.msg.client.jms.JmsFactoryFactory;
import com.ibm.msg.client.wmq.WMQConstants;

import javax.jms.JMSContext;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class Consumer {

    public static void main(String[] args) {
        Consumer consumer = new Consumer();
        consumer.initialize();
    }

    public void initialize() {
        LinkedBlockingQueue<String> receivedMessagesQueue = new LinkedBlockingQueue<>();
        setupMQListener(receivedMessagesQueue);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.execute(new ConsumerWorker(receivedMessagesQueue));

        // we prevent the executor to execute any further tasks
        executor.shutdown();

        // terminate actual (running) tasks
        try {
            if(!executor.awaitTermination(1000, TimeUnit.MILLISECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
        }
    }

    private void setupMQListener(Queue<String> receivedMessagesQueue) {
        try {
            JMSContext context = getJmsConnetionFactory().createContext();
            context.createConsumer(context.createQueue("DEV.QUEUE.1"))
                    .setMessageListener(message -> collectMessage(message, receivedMessagesQueue));

        } catch (JMSException e) {
            e.printStackTrace();
        }
    }

    private void collectMessage(Message message, Queue<String> receivedMessagesQueue) {
        if (message != null && message instanceof TextMessage) {
            try {
                receivedMessagesQueue.add(((TextMessage)message).getText());
            } catch (JMSException e) {
                e.printStackTrace();
            }
        }
    }

    private JmsConnectionFactory getJmsConnetionFactory() throws JMSException {

        JmsFactoryFactory ff = JmsFactoryFactory.getInstance(WMQConstants.WMQ_PROVIDER);
        JmsConnectionFactory cf = ff.createConnectionFactory();

        cf.setStringProperty(WMQConstants.WMQ_HOST_NAME, "localhost");
        cf.setIntProperty(WMQConstants.WMQ_PORT, 1414);
        cf.setStringProperty(WMQConstants.WMQ_CHANNEL, "DEV.APP.SVRCONN");
        cf.setIntProperty(WMQConstants.WMQ_CONNECTION_MODE, WMQConstants.WMQ_CM_CLIENT);
        cf.setStringProperty(WMQConstants.WMQ_QUEUE_MANAGER, "QM1");
        cf.setStringProperty(WMQConstants.WMQ_APPLICATIONNAME, "JMS-Test");
        cf.setBooleanProperty(WMQConstants.USER_AUTHENTICATION_MQCSP, true);
        cf.setStringProperty(WMQConstants.USERID, "app");
        cf.setStringProperty(WMQConstants.PASSWORD, "passw0rd");

        return cf;
    }
}
