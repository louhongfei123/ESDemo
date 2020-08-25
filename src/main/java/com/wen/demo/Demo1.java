package com.wen.demo;

import com.wen.utils.ESClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.junit.Test;

public class Demo1 {

    /**
     * 测试链接
     */
    @Test
    public void testConnect(){
        RestHighLevelClient client = ESClient.getRestHighLevelClient();
        System.out.println("123");

    }
}
