package com.example.rocketmqdemo02producer;

import com.alibaba.fastjson.JSON;
import com.example.rocketmqdemo02producer.domain.MsgModel;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.apache.rocketmq.spring.support.RocketMQHeaders;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

import java.util.Arrays;
import java.util.List;

@SpringBootTest
@Slf4j(topic = "RocketmqDemo02ProducerApplicationTests")
class RocketmqDemo02ProducerApplicationTests {
    @Autowired
    private RocketMQTemplate rocketMQTemplate;

    @Test
    void contextLoads() {
        // 同步
        rocketMQTemplate.syncSend("bootTestTopic", "我是boot的一个消息");

        // 异步
        rocketMQTemplate.asyncSend("bootTestTopic", "我是boot的一个异步消息", new SendCallback() {
            @Override
            public void onSuccess(SendResult sendResult) {
                log.info("发送成功");
            }

            @Override
            public void onException(Throwable throwable) {
                log.error("发送失败");
            }
        });

        // 单向
        rocketMQTemplate.sendOneWay("bootOneWayTopic", "单向消息");
        //延迟
        Message<String> msg = MessageBuilder.withPayload("我是一个延迟消息").build();
        rocketMQTemplate.syncSend("bootMsTopic",msg,3000,3);

        // 顺序消息
        List<MsgModel> msgModels = Arrays.asList(
                new MsgModel("qwer", 1, "下单"),
                new MsgModel("qwer", 1, "短信"),
                new MsgModel("qwer", 1, "物流"),
                new MsgModel("dsaf", 2, "下单"),
                new MsgModel("dsaf", 2, "短信"),
                new MsgModel("dsaf", 2, "物流")
        );
        msgModels.forEach((msgModel -> {
            rocketMQTemplate.syncSendOrderly("bootOrderlyTopic", JSON.toJSONString(msgModel),msgModel.getOrderSn());
        }));
    }
    @Test
    void tagProducerTest() {
        rocketMQTemplate.syncSend("bootTagTopic:tagA","我是一个带tagA的消息");
        // key是写在消息头的
        Message<String> msg = MessageBuilder.withPayload("我是一个带key的消息").setHeader(RocketMQHeaders.KEYS, "sdadjlkdsf").build();
        rocketMQTemplate.syncSend("bootTagTopic:tagA",msg);
    }

    @Test
    void modeTest() {
        for (int i = 0; i < 10; i++) {
            rocketMQTemplate.syncSend("modeTopic","我是第" + i + "个消息");
        }
    }
}