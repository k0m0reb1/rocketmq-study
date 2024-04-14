package com.sjtu.rocketmqdemo01;

import com.sjtu.rocketmqdemo01.constants.MqConstant;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.junit.jupiter.api.Test;
import org.postgresql.util.PSQLException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.*;
import java.util.UUID;

@SpringBootTest
@Slf4j(topic = "RocketmqDemo01ApplicationTests")
class RocketmqDemo01ApplicationTests {
    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Test
    void contextLoads() {

    }

    @Test
    void repeatProducer() throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("repeat-producer-group");
        producer.setNamesrvAddr(MqConstant.NAME_SRV_ADDR);
        producer.start();
        String key = UUID.randomUUID().toString();
        log.info("key:{}", key);
        Message msg1 = new Message("repeatTopic", null, key, "扣减库存-1".getBytes());
        Message msg1repeat = new Message("repeatTopic", null, key, "扣减库存-1".getBytes());
        producer.send(msg1);
        producer.send(msg1repeat);
        log.info("发送成功");
        producer.shutdown();

    }
    /*
    设计一个去重表，对消息唯一的key添加唯一索引
    每次消费消息的时候，先插入数据库，如果成功则执行业务逻辑
    如果插入失败，则说明消息来过了，直接签收了。
     */
    @Test
    void repeatConsumer() throws Exception {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("repeat-consumer");
        consumer.setNamesrvAddr(MqConstant.NAME_SRV_ADDR);
        consumer.subscribe("repeatTopic", "*");
        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            MessageExt msg = msgs.get(0);
            String keys = msg.getKeys();
            Connection conn = null;
            try {
                conn = DriverManager.getConnection("jdbc:postgresql://192.168.124.20:15400/studentdb", "student", "student@ustb2020");
            } catch (SQLException e) {
                e.printStackTrace();
            }
            PreparedStatement preparedStatement = null;
            try {
                preparedStatement = conn.prepareStatement(String.format("insert into tborder (type, order_sn, username) values ('1','%s','123')", keys));
            } catch (SQLException e) {
                e.printStackTrace();
            }
            try {
                int j = preparedStatement.executeUpdate();
            } catch (SQLException e) {
                if (e instanceof PSQLException) {
                    log.error("消息消费过");
                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                }
                e.printStackTrace();
            }
            log.info("消息体：{},key:{}", new String(msg.getBody()), keys);
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        consumer.start();
        System.in.read();

    }


}
