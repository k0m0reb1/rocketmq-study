package com.sjtun.rocketmqdemo03consumer.listener;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Repository;

/**
 * @author ：Fang Jiangjing
 * @date ：Created in 2024/4/14 13:27
 * @description：
 * @modified By：
 * @version: $
 */
@Component
@Slf4j(topic = "ABootSimpleListener")
@RocketMQMessageListener(topic = "bootTestTopic",consumerGroup = "boot-test-consumer-group")
public class ABootSimpleListener implements RocketMQListener<MessageExt> {

    @Override
    public void onMessage(MessageExt msg) {
        log.info("消息内容：{}",new String(msg.getBody()));
    }
}
