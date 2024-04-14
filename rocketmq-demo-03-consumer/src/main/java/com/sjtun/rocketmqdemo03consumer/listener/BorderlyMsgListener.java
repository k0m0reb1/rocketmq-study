package com.sjtun.rocketmqdemo03consumer.listener;

import com.alibaba.fastjson2.JSON;
import com.sjtun.rocketmqdemo03consumer.domain.MsgModel;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.spring.annotation.ConsumeMode;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.stereotype.Component;

/**
 * @author ：Fang Jiangjing
 * @date ：Created in 2024/4/14 14:13
 * @description：
 * @modified By：
 * @version: $
 */
@Slf4j(topic = "BorderlyMsgListener")
@Component
@RocketMQMessageListener(topic = "bootOrderlyTopic",consumerGroup = "boot-orderly-consumer-group",consumeMode = ConsumeMode.ORDERLY)
public class BorderlyMsgListener implements RocketMQListener<MessageExt> {
    @Override
    public void onMessage(MessageExt message) {
        MsgModel msgModel = JSON.parseObject(new String(message.getBody()), MsgModel.class);
        log.info(msgModel.toString());
    }
}
