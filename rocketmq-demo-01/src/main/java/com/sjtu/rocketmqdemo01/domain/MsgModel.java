package com.sjtu.rocketmqdemo01.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author ：Fang Jiangjing
 * @date ：Created in 2024/4/13 20:43
 * @description：
 * @modified By：
 * @version: $
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class MsgModel {
    private String orderSn;
    private Integer userId;
    private String desc;
}
