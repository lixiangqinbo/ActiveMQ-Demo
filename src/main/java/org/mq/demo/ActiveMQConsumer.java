package org.mq.demo;

import org.apache.activemq.ActiveMQConnectionFactory;
import javax.jms.*;
import java.io.IOException;

public class ActiveMQConsumer {
    public static final String ACTIVEMQ_URL = "tcp://162.14.77.50:61616";
    public static final String QUEUE_NAME = "queue01";   // 1对1 的队列

    public static void main(String[] args) throws JMSException, IOException {
        // 1. 创建工厂
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(ACTIVEMQ_URL);
        //2. 通过工厂返回连接，并且启动
        Connection connection = connectionFactory.createConnection();
        connection.start();
        //3. 创建会话 | 参数1 事务 参数2 告知收到设置
        Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        //4. 创建队列，队列名称
        Queue queue = session.createQueue(QUEUE_NAME);
        //5. 创建消息消费者
        MessageConsumer consumer = session.createConsumer(queue);
        while (true){
            //6. 通过消费者接收消息
            TextMessage receive = (TextMessage) consumer.receive();
            //手动签收 ：Session.CLIENT_ACKNOWLEDGE ::消费者收到每一条消息都要确认签收信号
            // testMessage.acknowledge();否则会重复消费
            receive.acknowledge();
            if (receive!=null){
                System.out.println("消费者消费"+QUEUE_NAME+":"+receive);
            }else {
                break;
            }
        }
        //7. 关闭资源
        consumer.close();
        session.close();
        connection.close();
        System.out.println("消费者向"+QUEUE_NAME+"消费消息结束");
    }

}
