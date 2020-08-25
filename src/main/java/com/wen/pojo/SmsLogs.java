package com.wen.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class SmsLogs {

    private String id;
    private Date createDate;  //创建时间String
    private Date sendDate;  //发送时间 date
    private String longCode;  //发送长号码 如 16092389287811
    private String mobile;  //如 13000000000
    private String corpName; //发送公司名称，需要分词检索
    private String smsContent;  //下发短信内容，需要分词检索
    private Integer state;   //短信下发状态 0 成功 1 失败 integer
    private Integer operatorId;  //运营商编号1移动2联通3电信 integer
    private String province;  //省份
    private String ipAddr;   //下发服务器IP地址
    private Integer replyTotal;  //短信状态报告返回时长 integer
    private Long fee;   // 	扣费 integer


}
