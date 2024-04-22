package com.sjtun.rocketmqdemo03consumer.listener;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.spring.annotation.MessageModel;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.annotation.SelectorType;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.stereotype.Component;

/**
 * @author ：Fang Jiangjing
 * @date ：Created in 2024/4/14 14:36
 * @description：
 * @modified By：
 * @version: $
 */
@Slf4j(topic = "DC1")
@Component
@RocketMQMessageListener(topic = "modeTopic",consumerGroup = "mode-consumer-group-a",
        messageModel = MessageModel.CLUSTERING,
        consumeThreadMax = 20)
public class DC1 implements RocketMQListener<String> {
    @Override
    public void onMessage(String s) {
        log.info("我是mode-consumer-group-a组的第一个消费者，msg:{}",s);
    }
}
