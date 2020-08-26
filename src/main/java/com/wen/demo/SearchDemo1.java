package com.wen.demo;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.wen.utils.ESClient;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.Test;

import java.util.Map;

public class SearchDemo1 {

    ObjectMapper mapper = new ObjectMapper();
    RestHighLevelClient client = ESClient.getRestHighLevelClient();
    String index = "sms-logs-index";
    String type="sms-logs-type";


    /**
     * term查询
     * @throws Exception
     */
    @Test
    public void termSerach()throws Exception{
        //构建查询请求对象
        SearchRequest request = new SearchRequest(index);
        request.types(type);
        SearchSourceBuilder builder = new SearchSourceBuilder();
        //分页
        builder.from(0);
        builder.size(5);
        //使用term查询
        builder.query(QueryBuilders.termQuery("province","北京"));
        request.source(builder);

        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        SearchHit[] hits = response.getHits().getHits();
        for(SearchHit hit:hits){
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            System.out.println(sourceAsMap);
        }
    }

    /**
     * terms查询
     * @throws Exception
     */
    @Test
    public void termsSerach()throws Exception{
        //构建查询请求对象
        SearchRequest request = new SearchRequest(index);
        request.types(type);
        SearchSourceBuilder builder = new SearchSourceBuilder();
        //使用terms查询
        builder.query(QueryBuilders.termsQuery("province","北京","上海"));
        request.source(builder);
        //获取返回值
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        for(SearchHit hit : response.getHits().getHits()){
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            System.out.println(sourceAsMap);
        }
    }

}
