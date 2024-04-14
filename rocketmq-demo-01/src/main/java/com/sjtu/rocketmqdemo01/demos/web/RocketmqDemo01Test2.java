package com.sjtu.rocketmqdemo01.demos.web;

import com.sjtu.rocketmqdemo01.constants.MqConstant;
import com.sjtu.rocketmqdemo01.domain.MsgModel;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.junit.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;


/**
 * @author ：Fang Jiangjing
 * @date ：Created in 2024/4/13 19:13
 * @description：
 * @modified By：
 * @version: $
 */
@Slf4j(topic = "RocketmqDemo01Test2")
public class RocketmqDemo01Test2 {
    private List<MsgModel> msgModels = Arrays.asList(
            new MsgModel("qwer", 1, "下单"),
            new MsgModel("qwer", 1, "短信"),
            new MsgModel("qwer", 1, "物流"),
            new MsgModel("dsaf", 2, "下单"),
            new MsgModel("dsaf", 2, "短信"),
            new MsgModel("dsaf", 2, "物流")
    );
    @Test
    public void orderProducer() throws Exception {
        // 创建一个生产者
        DefaultMQProducer producer = new DefaultMQProducer("orderly-producer-group");
        // 连接nameserver
        producer.setNamesrvAddr(MqConstant.NAME_SRV_ADDR);
        // 启动producter
        producer.start();
        // 发送顺序消息 发送时要确保有序，并且要发送到同一个队列里面去
        msgModels.forEach((msgModel) -> {
            Message msg = new Message("orderlyTopic",msgModel.toString().getBytes());
            // 发相同的订单号去相同的队列
            try {
                producer.send(msg, new MessageQueueSelector() {
                    @Override
                    public MessageQueue select(List<MessageQueue> list, Message message, Object o) {
                        int hashCode = o.toString().hashCode();
                        int i = hashCode % list.size();
                        return list.get(i);
                    }
                },msgModel.getOrderSn());
            } catch (MQClientException e) {
                e.printStackTrace();
            } catch (RemotingException e) {
                e.printStackTrace();
            } catch (MQBrokerException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });

        producer.shutdown();
        log.info("发送完毕");
    }

    @Test
    public void orderConsumer() throws Exception{
        // 创建一个消费组
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("orderly-consumer-group");
        consumer.setNamesrvAddr(MqConstant.NAME_SRV_ADDR);
        // 订阅一个主题,*标识订阅这个主题中所有消息
        consumer.subscribe("orderlyTopic","*");
        // 设置一个监听器
        consumer.registerMessageListener((MessageListenerOrderly) (msgs, context) -> {
            log.info("线程id:{}",Thread.currentThread().getId());
            msgs.forEach((msg) -> log.info("消息内容:{}",new String(msg.getBody())));
            log.info("消费上下文:{}",context);
            // 返回值 CONSUME_SUCCESS成功，消息会从mq出队
            // RECONSUME_LATER(报错/null) 失败，消息会重新回到队列，过一会重新投递出来，给当前消费者或其他消费者消费
            return ConsumeOrderlyStatus.SUCCESS;
        });
        consumer.start();
        // 挂起当前jvm
        System.in.read();
    }
}
