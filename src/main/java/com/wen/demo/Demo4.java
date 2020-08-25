package com.wen.demo;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.wen.pojo.Person;
import com.wen.utils.ESClient;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.junit.Test;

import java.util.Date;

public class Demo4 {

    ObjectMapper mapper = new ObjectMapper();
    RestHighLevelClient client = ESClient.getRestHighLevelClient();
    String index = "person";
    String type="man";


    /**
     * 创建多个文档
     * @throws Exception
     */
    @Test
    public void createMore()throws Exception{


        Person p1 = new Person(1,"占山",23,new Date());
        Person p2 = new Person(2,"里斯",23,new Date());
        Person p3 = new Person(3,"王五",23,new Date());
        String josn1 = mapper.writeValueAsString(p1);
        String josn2 = mapper.writeValueAsString(p2);
        String josn3 = mapper.writeValueAsString(p3);
        BulkRequest request = new BulkRequest();
        request.add(new IndexRequest(index,type,p1.getId().toString()).source(josn1, XContentType.JSON));
        request.add(new IndexRequest(index,type,p2.getId().toString()).source(josn2, XContentType.JSON));
        request.add(new IndexRequest(index,type,p3.getId().toString()).source(josn3, XContentType.JSON));
        BulkResponse bulk = client.bulk(request, RequestOptions.DEFAULT);
        System.out.println(bulk.toString());

    }

    /**
     * 删除多个文档
     * @throws Exception
     */
    @Test
    public void delMore()throws Exception{

        BulkRequest request = new BulkRequest();
        request.add(new DeleteRequest(index,type,"1"));
        request.add(new DeleteRequest(index,type,"2"));
        request.add(new DeleteRequest(index,type,"3"));
        BulkResponse bulk = client.bulk(request, RequestOptions.DEFAULT);
        System.out.println(bulk.toString());
    }

    /**
     * 创建索引
     * @throws Exception
     */
    @Test
    public void createIndex() throws  Exception{
        String index = "sms-logs-index";
        String type="sms-logs-type";
        Settings.Builder setting = Settings.builder().put("number_of_shards", "5").put("number_of_replicas", 1);
        XContentBuilder mappings = JsonXContent.contentBuilder()
                .startObject()
                .startObject("properties")
                .startObject("corpName")
                .field("type", "keyword")
                .endObject()
                .startObject("createDate")
                .field("type", "date")
                .field("format", "yyyy-MM-dd")
                .endObject()
                .startObject("fee")
                .field("type", "long")
                .endObject()
                .startObject("ipAddr")
                .field("type", "ip")
                .endObject()
                .startObject("longCode")
                .field("type", "keyword")
                .endObject()
                .startObject("mobile")
                .field("type", "keyword")
                .endObject()
                .startObject("operatorId")
                .field("type", "integer")
                .endObject()
                .startObject("province")
                .field("type", "keyword")
                .endObject()
                .startObject("replyTotal")
                .field("type", "integer")
                .endObject()
                .startObject("sendDate")
                .field("type", "date")
                .field("format", "yyyy-MM-dd")
                .endObject()
                .startObject("smsContent")
                .field("type", "text")
                .field("analyzer", "ik_max_word")
                .endObject()
                .startObject("state")
                .field("type", "integer")
                .endObject()
                .endObject()
                .endObject();
        CreateIndexRequest request = new CreateIndexRequest(index).settings(setting).mapping(type,mappings);
        CreateIndexResponse response = client.indices().create(request, RequestOptions.DEFAULT);
        System.out.println(response.toString());
    }


}
