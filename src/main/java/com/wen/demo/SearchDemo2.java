package com.wen.demo;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.wen.utils.ESClient;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.Test;

import java.util.Map;

public class SearchDemo2 {

    ObjectMapper mapper = new ObjectMapper();
    RestHighLevelClient client = ESClient.getRestHighLevelClient();
    String index = "sms-logs-index";
    String type="sms-logs-type";

    /**
     * 根据id查询
     * @throws Exception
     */
    @Test
    public void findById()throws Exception{
        GetRequest request = new GetRequest(index,type,"1");
        GetResponse response = client.get(request, RequestOptions.DEFAULT);
        Map<String, Object> sourceAsMap = response.getSourceAsMap();
        System.out.println(sourceAsMap);

    }

    /**
     * ids查询
     * @throws Exception
     */
    @Test
    public void findByIds()throws Exception{
        SearchRequest request = new SearchRequest(index);
        request.types(type);
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.query(QueryBuilders.idsQuery().addIds("1","10"));
        request.source(builder);
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        SearchHit[] hits = response.getHits().getHits();
        System.out.println(hits.length);
        for(SearchHit hit : hits){
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            System.out.println(sourceAsMap);
        }
    }

    /**
     * 前缀查询
     * @throws Exception
     */
    @Test
    public void prefixSearch()throws Exception{
        SearchRequest request = new SearchRequest(index);
        request.types(type);
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.query(QueryBuilders.prefixQuery("corpName","阿里"));
        request.source(builder);
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        SearchHit[] hits = response.getHits().getHits();
        System.out.println(hits.length);
        for(SearchHit hit : hits){
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            System.out.println(sourceAsMap);
        }
    }

    /**
     * fuzzy查询
     * @throws Exception
     */
    @Test
    public void fuzzySearch()throws Exception{
        SearchRequest request = new SearchRequest(index);
        request.types(type);
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.query(QueryBuilders.fuzzyQuery("corpName","格力骑车").prefixLength(0));
        request.source(builder);
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        SearchHit[] hits = response.getHits().getHits();
        System.out.println(hits.length);
        for(SearchHit hit : hits){
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            System.out.println(sourceAsMap);
        }
    }

    /**
     * wildcard查询
     * @throws Exception
     */
    @Test
    public void wildcardSearch()throws Exception{
        SearchRequest request = new SearchRequest(index);
        request.types(type);
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.query(QueryBuilders.wildcardQuery("corpName","格力??"));
        //builder.query(QueryBuilders.wildcardQuery("corpName","格力*"));
        request.source(builder);
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        SearchHit[] hits = response.getHits().getHits();
        System.out.println(hits.length);
        for(SearchHit hit : hits){
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            System.out.println(sourceAsMap);
        }
    }

    /**
     * 范围查询
     * @throws Exception
     */
    @Test
    public void rangeSearch()throws Exception{
        SearchRequest request = new SearchRequest(index);
        request.types(type);
        SearchSourceBuilder builder = new SearchSourceBuilder();

        //查询范围:[gte,lte]
        builder.query(QueryBuilders.rangeQuery("fee").gte("10").lte("20"));
        //查询范围：(gt,lt)
        //builder.query(QueryBuilders.rangeQuery("fee").gt("10").lt("20"));
        request.source(builder);
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        SearchHit[] hits = response.getHits().getHits();
        System.out.println(hits.length);
        for(SearchHit hit : hits){
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            System.out.println(sourceAsMap);
        }
    }

    /**
     * regexp查询
     * @throws Exception
     */
    @Test
    public void regexpSearch()throws Exception{
        SearchRequest request = new SearchRequest(index);
        request.types(type);
        SearchSourceBuilder builder = new SearchSourceBuilder();

        builder.query(QueryBuilders.regexpQuery("mobile","138[0-9]{7}"));
        request.source(builder);
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        SearchHit[] hits = response.getHits().getHits();
        System.out.println(hits.length);
        for(SearchHit hit : hits){
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            System.out.println(sourceAsMap);
        }
    }








}
