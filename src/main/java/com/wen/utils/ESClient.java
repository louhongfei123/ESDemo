package com.wen.utils;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;

public class ESClient {



    public static RestHighLevelClient getRestHighLevelClient(){
        HttpHost httpHost = new HttpHost("192.168.200.144",9200);
        RestClientBuilder restClientBuilder = RestClient.builder(httpHost);
        return  new RestHighLevelClient(restClientBuilder);
    }
}
