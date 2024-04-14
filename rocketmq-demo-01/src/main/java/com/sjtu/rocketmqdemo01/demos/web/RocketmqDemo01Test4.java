package com.sjtu.rocketmqdemo01.demos.web;

import com.sjtu.rocketmqdemo01.constants.MqConstant;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.junit.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.UUID;

/**
 * @author ：Fang Jiangjing
 * @date ：Created in 2024/4/14 10:43
 * @description：
 * @modified By：
 * @version: $
 */
@Slf4j(topic = "RocketmqDemo01Test4")
public class RocketmqDemo01Test4 {
    @Test
    public void retryProducer() throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("retry-producer-group");
        producer.setNamesrvAddr(MqConstant.NAME_SRV_ADDR);
        producer.setRetryTimesWhenSendFailed(2);
        producer.setRetryTimesWhenSendAsyncFailed(2);
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

    /**
     *     重试的时间间隔
     *     10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h
     *     默认重试16次
     *     1. 可以自定义重试次数
     *     2. 如果重试了16次（并发模式),顺序模式下（int最大值）都是失败的，是一个死信消息，则会放到一个死信主题中去 %DLQ%retry-consumer-group3
     *     3. 当消息处理失败的时候，该如何正确的处理
     */
    @Test
    public void repeatConsumer() throws Exception{
        // 创建一个消费组
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("retry-consumer-group");
        consumer.setNamesrvAddr(MqConstant.NAME_SRV_ADDR);
        // 订阅一个主题,*标识订阅这个主题中所有消息
        consumer.subscribe("repeatTopic","*");
        consumer.setMaxReconsumeTimes(2);
        // 设置一个监听器
        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            msgs.forEach((msg) -> log.info("消息内容:{},业务key:{},消费时间:{},重试次数:{}",new String(msg.getBody()),new String(msg.getKeys()),LocalDateTime.now(),msg.getReconsumeTimes()));
//            log.info("消费上下文:{}",context);
            // 返回值 CONSUME_SUCCESS成功，消息会从mq出队
            // RECONSUME_LATER(报错/null) 失败，消息会重新回到队列，过一会重新投递出来，给当前消费者或其他消费者消费
            return ConsumeConcurrentlyStatus.RECONSUME_LATER;
        });
        consumer.start();
        // 挂起当前jvm
//        System.in.read();
        Thread.sleep(1000*300);
        consumer.shutdown();
    }

    @Test
    public void retryDeadConsumer() throws Exception{
        // 创建一个消费组
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("retrydead-consumer-group");
        consumer.setNamesrvAddr(MqConstant.NAME_SRV_ADDR);
        // 订阅一个主题,*标识订阅这个主题中所有消息
        consumer.subscribe("%DLQ%retry-consumer-group","*");
        // 设置一个监听器
        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            msgs.forEach((msg) -> log.info("消息内容:{},业务key:{},消费时间:{},重试次数:{}",new String(msg.getBody()),new String(msg.getKeys()),LocalDateTime.now(),msg.getReconsumeTimes()));
//            log.info("消费上下文:{}",context);
            // 返回值 CONSUME_SUCCESS成功，消息会从mq出队
            // RECONSUME_LATER(报错/null) 失败，消息会重新回到队列，过一会重新投递出来，给当前消费者或其他消费者消费
            log.error("记录到特别的位置，通知人工来处理。");
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        consumer.start();
        // 挂起当前jvm
//        System.in.read();
        Thread.sleep(1000*300);
        consumer.shutdown();
    }

    @Test
    public void retryOptimizeConsumer() throws Exception{
        // 创建一个消费组
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("retryoptimize-consumer-group");
        consumer.setNamesrvAddr(MqConstant.NAME_SRV_ADDR);
        // 订阅一个主题,*标识订阅这个主题中所有消息
        consumer.subscribe("repeatTopic","*");
        // 设置一个监听器
        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            MessageExt msg = msgs.get(0);
            try{
                int i = 100 / 0;
            } catch (Exception e) {
                int recosumeTimes = msg.getReconsumeTimes();
                if (recosumeTimes >= 3) {
                    log.error("记录到特别的位置，等待人工处理");
                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                }
                return ConsumeConcurrentlyStatus.RECONSUME_LATER;
            }

            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        consumer.start();
        // 挂起当前jvm
//        System.in.read();
        Thread.sleep(1000*300);
        consumer.shutdown();
    }
}
