package org.mq.demo.topic;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;
import java.util.UUID;

public class ActiveMQTopicProvider {

    public static final String ACTIVEMQ_URL = "tcp://162.14.77.50:61616";
    public static final String TOPIC_NAME = "topic01";   // 1对N 的主题

    public static void main(String[] args) throws JMSException {
        //创建消息连接工厂
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(ACTIVEMQ_URL);
        //创建一个连接 后续使用完需要关闭
        Connection connection = connectionFactory.createConnection();
        connection.start();
        //通过连接创建一次会话 开启事务 使用自动签收模式 开启事务必须提交 错误就回滚
        Session session = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);
        //创建主题
        Topic topic = session.createTopic(TOPIC_NAME);

        //创建一个消息的提供者
        MessageProducer producer = session.createProducer(topic);
        for (int i = 0; i <3 ; i++) {
            //创建消息类型并向topic发送消息
            TextMessage textMessage = session.createTextMessage("生产者创建消息" + UUID.randomUUID().toString().substring(0, 5));
            producer.send(textMessage);
        }
        try {
            session.commit();//提交事务 不提交MQ会重试6次，间隔一秒 之后消息失败
        }catch (RuntimeException runtimeException){
            session.rollback();
        }
        //关闭资源
        producer.close();
        session.close();
        connection.close();
        System.out.println("生产者向"+TOPIC_NAME+"发送消息结束");
    }
}
