package com.sjtun.rocketmqdemo03consumer.listener;

import com.alibaba.fastjson2.JSON;
import com.sjtun.rocketmqdemo03consumer.domain.MsgModel;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.spring.annotation.ConsumeMode;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.annotation.SelectorType;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.stereotype.Component;

/**
 * @author ：Fang Jiangjing
 * @date ：Created in 2024/4/14 14:25
 * @description：
 * @modified By：
 * @version: $
 */
@Slf4j(topic = "CTagMsgListener")
@Component
@RocketMQMessageListener(topic = "bootTagTopic",consumerGroup = "boot-tag-consumer-group",
                        selectorType = SelectorType.TAG,
                        selectorExpression = "tagA || tagB")
public class CTagMsgListener implements RocketMQListener<MessageExt> {
    @Override
    public void onMessage(MessageExt message) {
        log.info(new String(message.getBody()));
        log.info(message.getKeys());
    }
}
