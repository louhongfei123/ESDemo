package com.wen.demo;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.wen.utils.ESClient;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.BoostingQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
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

    /**
     * scroll深分页查询
     * @throws Exception
     */
    @Test
    public void scrollSearch()throws Exception{
        SearchRequest request = new SearchRequest(index);
        request.types(type);
        SearchSourceBuilder builder = new SearchSourceBuilder();

        //构建scroll条件
        request.scroll(TimeValue.timeValueMinutes(1));
        builder.size(3);
        builder.sort("fee", SortOrder.DESC);
        builder.query(QueryBuilders.matchAllQuery());
        request.source(builder);

        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        String scrollId = response.getScrollId();
        SearchHit[] hits = response.getHits().getHits();
        System.out.println("第一页");
        for(SearchHit hit : hits){
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            System.out.println(sourceAsMap);
        }

        //循环查询下一页
        while (true){
            SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
            //查询下一页时，续上scrollId在内存的存在时间
            scrollRequest.scroll(TimeValue.timeValueMinutes(1));
            SearchResponse searchResponse = client.scroll(scrollRequest, RequestOptions.DEFAULT);
            if(searchResponse.getHits().getHits() != null && searchResponse.getHits().getHits().length > 0){
                System.out.println("下一页");
                for (SearchHit hit : searchResponse.getHits().getHits()) {
                    Map<String, Object> sourceAsMap = hit.getSourceAsMap();
                    System.out.println(sourceAsMap);
                }
            }else{
                System.out.println("结束");
                break;
            }
        }
        //清除es缓存，根据scrollId，因为保存了一分钟
        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        clearScrollRequest.addScrollId(scrollId);
        client.clearScroll(clearScrollRequest,RequestOptions.DEFAULT);

    }


    /**
     * deleteByQuery
     * @throws Exception
     */
    @Test
    public void deleteByQuery()throws Exception{
        DeleteByQueryRequest request = new DeleteByQueryRequest(index);
        request.types(type);
        request.setQuery(QueryBuilders.rangeQuery("fee").lt(20));
        BulkByScrollResponse response = client.deleteByQuery(request, RequestOptions.DEFAULT);
        System.out.println(response);


    }

    /**
     * bool复合查询
     * @throws Exception
     */
    @Test
    public void boolSearch()throws Exception{
        SearchRequest request = new SearchRequest(index);
        request.types(type);
        SearchSourceBuilder builder = new SearchSourceBuilder();
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        //北京或者上海
        boolQueryBuilder.should(QueryBuilders.termQuery("province","北京"));
        boolQueryBuilder.should(QueryBuilders.termQuery("province","上海"));
        //排除operatorId为2
        boolQueryBuilder.mustNot(QueryBuilders.termQuery("operatorId","2"));
        //smsContent中包含一群与孩童
        boolQueryBuilder.must(QueryBuilders.matchQuery("smsContent","一群"));
        boolQueryBuilder.must(QueryBuilders.matchQuery("smsContent","孩童"));
        builder.query(boolQueryBuilder);
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
     * boosting查询
     * @throws Exception
     */
    @Test
    public void boostingSearch()throws Exception {
        SearchRequest request = new SearchRequest(index);
        request.types(type);
        SearchSourceBuilder builder = new SearchSourceBuilder();
        BoostingQueryBuilder boostingQueryBuilder = QueryBuilders.boostingQuery(
                QueryBuilders.matchQuery("smsContent", "孩童"),
                QueryBuilders.matchQuery("smsContent", "希尔曼")
        );
        boostingQueryBuilder.negativeBoost(0.5F);
        builder.query(boostingQueryBuilder);
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
     * filter查询
     * @throws Exception
     */
    @Test
    public void filterSearch()throws Exception{
        SearchRequest request = new SearchRequest(index);
        request.types(type);
        SearchSourceBuilder builder = new SearchSourceBuilder();
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        //使用bool符合查询的filter查询
        boolQueryBuilder.filter(QueryBuilders.termQuery("corpName","格力汽车"));
        boolQueryBuilder.filter(QueryBuilders.rangeQuery("fee").gt(20));
        builder.query(boolQueryBuilder);
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
