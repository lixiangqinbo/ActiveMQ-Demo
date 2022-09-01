package org.mq.demo;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;
import java.util.UUID;

public class ActiveMQProvider {
    public static final String ACTIVEMQ_URL = "tcp://162.14.77.50:61616";
    public static final String QUEUE_NAME = "queue01";   // 1对1 的队列


    public static void main(String[] args) throws JMSException {
        //1. 创建工厂
        ActiveMQConnectionFactory activeMQConnectionFactory
                = new ActiveMQConnectionFactory(ACTIVEMQ_URL);
        //2. 通过工厂返回连接，并且启动
        Connection connection = activeMQConnectionFactory.createConnection();
        connection.start();
        //3. 创建会话 | 参数1 事务 参数2 告知收到设置
        Session session = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);
        //4. 创建队列，队列名称
        Queue queue = session.createQueue(QUEUE_NAME);
        //5. 创建消息生产者
        MessageProducer producer = session.createProducer(queue);
        /**
         * DeliveryMode.NON_PERSISTENT : 非持久化的传递方式
         * DeliveryMode.PERSISTENT ：持久化的传递方式
         */
        producer.setDeliveryMode(DeliveryMode.PERSISTENT);
        //6. 通过生产者生产消息发送到队列里面
        for (int i = 0; i <3 ; i++) {
            //7. 通过会话创建消息
            TextMessage textMessage = session.createTextMessage("msg：：：" +
                    UUID.randomUUID().toString().substring(0,5));
            //8. 通过生产者发送给mq
            producer.send(textMessage);
            session.commit();
        }
        //9. 关闭资源
        producer.close();
        session.close();
        connection.close();
        System.out.println("生产者向"+QUEUE_NAME+"发送消息结束");

    }


}
