package com.wen.utils;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;


public class ESClient {


    /**
     * 建立连接
     * @return
     */
    public static RestHighLevelClient getRestHighLevelClient(){
        HttpHost httpHost = new HttpHost("IP地址",9200);
        RestClientBuilder restClientBuilder = RestClient.builder(httpHost);
        return  new RestHighLevelClient(restClientBuilder);
    }
}
