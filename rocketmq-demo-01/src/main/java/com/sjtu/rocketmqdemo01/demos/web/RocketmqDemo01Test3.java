package com.sjtu.rocketmqdemo01.demos.web;

import com.sjtu.rocketmqdemo01.constants.MqConstant;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.junit.Test;

/**
 * @author ：Fang Jiangjing
 * @date ：Created in 2024/4/13 21:47
 * @description：
 * @modified By：
 * @version: $
 */
@Slf4j(topic = "RocketmqDemo01Test3")
public class RocketmqDemo01Test3 {
    @Test
    public void tagProducer() throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("tag-producer-group");
        producer.setNamesrvAddr(MqConstant.NAME_SRV_ADDR);
        producer.start();
        Message msg1 = new Message("tagTopic","vip1","我是vip1的文章".getBytes());
        Message msg2 = new Message("tagTopic","vip2","我是vip2的文章".getBytes());
        producer.send(msg1);
        producer.send(msg2);
        producer.shutdown();
        log.info("发送完毕");
    }

    @Test
    public void
    tagConsumer1() throws Exception{
        // 创建一个消费组
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("tag-consumer-group-a");
        consumer.setNamesrvAddr(MqConstant.NAME_SRV_ADDR);
        // 订阅一个主题,*标识订阅这个主题中所有消息
        consumer.subscribe("tagTopic","vip1");
        // 设置一个监听器
        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            msgs.forEach((msg) -> log.info("消息内容:{}",new String(msg.getBody())));
            log.info("消费上下文:{}",context);
            // 返回值 CONSUME_SUCCESS成功，消息会从mq出队
            // RECONSUME_LATER(报错/null) 失败，消息会重新回到队列，过一会重新投递出来，给当前消费者或其他消费者消费
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        consumer.start();
        // 挂起当前jvm
        System.in.read();
    }

    @Test
    public void tagConsumer2() throws Exception{
        // 创建一个消费组
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("tag-consumer-group-b");
        consumer.setNamesrvAddr(MqConstant.NAME_SRV_ADDR);
        // 订阅一个主题,*标识订阅这个主题中所有消息
        consumer.subscribe("tagTopic","vip1 || vip2");
        // 设置一个监听器
        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            msgs.forEach((msg) -> log.info("消息内容:{}",new String(msg.getBody())));
            log.info("消费上下文:{}",context);
            // 返回值 CONSUME_SUCCESS成功，消息会从mq出队
            // RECONSUME_LATER(报错/null) 失败，消息会重新回到队列，过一会重新投递出来，给当前消费者或其他消费者消费
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        consumer.start();
        // 挂起当前jvm
        System.in.read();
    }
    @Test
    public void keyProducer() throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("key-producer-group");
        producer.setNamesrvAddr(MqConstant.NAME_SRV_ADDR);
        producer.start();
        Message msg1 = new Message("keyTopic","vip1","key","我是vip1的文章".getBytes());
        SendResult send = producer.send(msg1);
        producer.shutdown();
        log.info("发送完毕:{}",send.getSendStatus());
    }
    @Test
    public void keyConsumer() throws Exception{
        // 创建一个消费组
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("key1-consumer-group");
        consumer.setNamesrvAddr(MqConstant.NAME_SRV_ADDR);
        // 订阅一个主题,*标识订阅这个主题中所有消息
        consumer.subscribe("keyTopic","vip1");
        // 设置一个监听器
        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            msgs.forEach((msg) -> log.info("消息内容:{},业务key:{}",new String(msg.getBody()),new String(msg.getKeys())));
//            log.info("消费上下文:{}",context);
            // 返回值 CONSUME_SUCCESS成功，消息会从mq出队
            // RECONSUME_LATER(报错/null) 失败，消息会重新回到队列，过一会重新投递出来，给当前消费者或其他消费者消费
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        consumer.start();
        // 挂起当前jvm
//        System.in.read();
        Thread.sleep(1000*100);
        consumer.shutdown();
    }
}
