package org.mq.demo.topic;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;
import java.io.IOException;

public class ActiveMQTopicConsumer {

    public static final String ACTIVEMQ_URL = "tcp://162.14.77.50:61616";
    public static final String TOPIC_NAME = "topic01";   // 1对N 的主题

    public static void main(String[] args) throws JMSException, IOException {
        //创建消息连接工厂
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(ACTIVEMQ_URL);
        //创建一个连接 后续使用完需要关闭
        Connection connection = connectionFactory.createConnection();
        connection.start();
        //通过连接创建一次会话 开启事务 使用自动签收模式 开启事务必须提交 错误就回滚
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        //创建主题
        Topic topic = session.createTopic(TOPIC_NAME);
        //创建一个消费者
        MessageConsumer consumer = session.createConsumer(topic);
        // 通过监听的方式来消费消息
        // 通过异步非阻塞的方式消费消息
        // consumer 的setMessageListener 注册一个监听器，
        // 当有消息发送来时，系统自动调用MessageListener 的 onMessage 方法处理消息
        consumer.setMessageListener((Message message)->{
            if (message!=null && message instanceof TextMessage){
                try {
                    TextMessage textMessage = (TextMessage)message;
                    System.out.println("消费者的消息："+textMessage.getText());
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }
        });
        //关闭资源
//        session.commit(); //todo 事务开启一直重复消费
        System.in.read();
        consumer.close();
        session.close();
        connection.close();
        System.out.println("消费者向"+TOPIC_NAME+"消费消息结束");
    }
}
