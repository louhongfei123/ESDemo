package com.wen.demo;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.wen.pojo.Person;
import com.wen.utils.ESClient;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.Test;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class Demo3 {

    ObjectMapper mapper = new ObjectMapper();
    RestHighLevelClient client = ESClient.getRestHighLevelClient();
    String index = "person";
    String type="man";

    /**
     * 创建文档
     * @throws Exception
     */
    @Test
    public void createDoc()throws Exception{
        Person person = new Person(1,"占山",23,new Date());
        String json = mapper.writeValueAsString(person);
        IndexRequest request = new IndexRequest(index,type,person.getId().toString());
        request.source(json, XContentType.JSON);
        IndexResponse response = client.index(request, RequestOptions.DEFAULT);
        System.out.println(response.getResult());
    }

    /**
     * 更新文档
     * @throws Exception
     */
    @Test
    public void update() throws Exception{
        Map<String,Object> doc = new HashMap<>();
        doc.put("name","123");
        String code = "1";
        UpdateRequest request = new UpdateRequest(index,type,code);
        request.doc(doc);
        UpdateResponse update = client.update(request, RequestOptions.DEFAULT);
        System.out.println(update.toString());
    }

    /**
     * 删除文档
     * @throws Exception
     */
    @Test
    public void del()throws Exception{
        DeleteRequest request = new DeleteRequest(index,type,"1");
        DeleteResponse delete = client.delete(request, RequestOptions.DEFAULT);
        System.out.println(delete.toString());
    }
}
