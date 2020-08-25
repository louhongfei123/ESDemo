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
    private Date createDate;
    private Date sendDate;
    private String longCode;
    private String mobile;
    private String corpName;
    private String smsContent;
    private Integer state;
    private Integer operatorId;
    private String province;
    private String ipAddr;
    private Integer replyTotal;
    private Long fee;


}
