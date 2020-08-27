package com.wen.demo;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.wen.utils.ESClient;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.junit.Test;

import java.util.Map;

public class SearchDemo3 {

    ObjectMapper mapper = new ObjectMapper();
    RestHighLevelClient client = ESClient.getRestHighLevelClient();
    String index = "sms-logs-index";
    String type="sms-logs-type";

    /**
     * 高亮查询
     * @throws Exception
     */
    @Test
    public void highLightSearch()throws Exception{
        SearchRequest request = new SearchRequest(index);
        request.types(type);
        SearchSourceBuilder builder = new SearchSourceBuilder();
        //构建查询
        builder.query(QueryBuilders.matchQuery("smsContent","团队"));
        // Highlight与query同级所以单独建立
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        //指定高亮
        highlightBuilder.field("smsContent",10).preTags("<font color='red'>").postTags("</font>");
        builder.highlighter(highlightBuilder);
        request.source(builder);
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        SearchHit[] hits = response.getHits().getHits();
        System.out.println(hits.length);
        for(SearchHit hit : hits){
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            //System.out.println(sourceAsMap);
            HighlightField smsContent = hit.getHighlightFields().get("smsContent");
            System.out.println(smsContent);

        }
    }

}
