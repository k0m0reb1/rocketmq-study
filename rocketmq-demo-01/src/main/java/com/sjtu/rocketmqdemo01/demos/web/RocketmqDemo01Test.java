package com.sjtu.rocketmqdemo01.demos.web;

import com.sjtu.rocketmqdemo01.constants.MqConstant;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.junit.Test;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;

/**
 * @author ：Fang Jiangjing
 * @date ：Created in 2024/4/13 18:10
 * @description：
 * @modified By：
 * @version: $
 */
@Slf4j(topic = "RocketmqDemo01Test")
public class RocketmqDemo01Test {
    @Test
    public void simpleProducer() throws MQBrokerException, RemotingException, InterruptedException, MQClientException {
        // 创建一个生产者
        DefaultMQProducer producer = new DefaultMQProducer("test-prod");
        // 连接nameserver
        producer.setNamesrvAddr(MqConstant.NAME_SRV_ADDR);
        // 启动producter
        producer.start();
        // 创建一个消息
        Message message = new Message("testTopic","我是一个简单消息".getBytes());
        SendResult sendResult = producer.send(message);
        log.info(sendResult.getSendStatus().toString());
        producer.shutdown();
    }

    @Test
    public void simpleConsumer() throws Exception{
        // 创建一个消费组
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("test-consumer-group");
        consumer.setNamesrvAddr(MqConstant.NAME_SRV_ADDR);
        // 订阅一个主题,*标识订阅这个主题中所有消息
        consumer.subscribe("testTopic","*");
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
    public void aysncProducer() throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("async-producer-group");
        producer.setNamesrvAddr(MqConstant.NAME_SRV_ADDR);
        producer.start();
        Message msg = new Message("asyncTopic","我是一个异步消息".getBytes());
        producer.send(msg, new SendCallback() {
            @Override
            public void onSuccess(SendResult sendResult) {
                log.info("发送成功");
            }

            @Override
            public void onException(Throwable throwable) {
                log.error("发送失败");
            }
        });
        System.in.read();
    }

    @Test
    public void onewayProcducer() throws Exception{
        DefaultMQProducer producer = new DefaultMQProducer("oneway-producer-group");
        producer.setNamesrvAddr(MqConstant.NAME_SRV_ADDR);
        producer.start();
        Message msg = new Message("onewayTopic","日志****".getBytes());
        producer.sendOneway(msg);
        log.info("发送成功");
        producer.shutdown();
    }

    @Test
    public void msProducer() throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("ms-producer-group");
        producer.setNamesrvAddr(MqConstant.NAME_SRV_ADDR);
        producer.start();
        Message msg = new Message("orderMsTopic", "订单号，座位号".getBytes());
        msg.setDelayTimeLevel(3);
        producer.send(msg);
        log.info("发送成功，时间:{}", LocalDateTime.now());
        producer.shutdown();
    }
    @Test
    public void msConsumer() throws Exception {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("ms-consumer-group");
        consumer.setNamesrvAddr(MqConstant.NAME_SRV_ADDR);
        consumer.subscribe("orderMsTopic","*");
        // 设置一个监听器
        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            log.info("收到消息了，时间：{}",LocalDateTime.now());
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
    public void bhProducer() throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("bh-producer-group");
        producer.setNamesrvAddr(MqConstant.NAME_SRV_ADDR);
        producer.start();
        List<Message> msgs = Arrays.asList(new Message("batchTopic", "我是一组消息的a消息".getBytes()),
                new Message("batchTopic", "我是一组消息的b消息".getBytes()),
                new Message("batchTopic", "我是一组消息的c消息".getBytes()));
        SendResult send = producer.send(msgs);
        log.info(send.toString());
        producer.shutdown();
    }

    @Test
    public void bhConsumer() throws Exception {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("bh-consumer-group");
        consumer.setNamesrvAddr(MqConstant.NAME_SRV_ADDR);
        consumer.subscribe("batchTopic","*");
        // 设置一个监听器
        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            log.info("收到消息了，时间：{}",LocalDateTime.now());
            log.info(String.valueOf(msgs.size()));
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
}
