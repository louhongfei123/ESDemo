package com.wen.demo;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.wen.pojo.Person;
import com.wen.pojo.SmsLogs;
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

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class Demo4 {

    ObjectMapper mapper = new ObjectMapper();
    RestHighLevelClient client = ESClient.getRestHighLevelClient();



    /**
     * 创建多个文档
     * @throws Exception
     */
    @Test
    public void createMore()throws Exception{
        String index = "person";
        String type="man";

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
        String index = "person";
        String type="man";
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

    /**
     * 添加测试数据
     * @throws Exception
     */
    @Test
    public void createDoc()throws  Exception{
        String index = "sms-logs-index";
        String type="sms-logs-type";
        String longcode = "1008687";
        String mobile ="138340658";
        List<String> companies = new ArrayList<>();
        companies.add("腾讯课堂");
        companies.add("阿里旺旺");
        companies.add("海尔电器");
        companies.add("海尔智家公司");
        companies.add("格力汽车");
        companies.add("苏宁易购");
        List<String> provinces = new ArrayList<>();
        provinces.add("北京");
        provinces.add("重庆");
        provinces.add("上海");
        provinces.add("晋城");
        BulkRequest bulkRequest = new BulkRequest();
        for (int i = 1; i <16 ; i++) {
            Thread.sleep(1000);
            SmsLogs s1 = new SmsLogs();
            s1.setId(i);
            s1.setCreateDate(new Date());
            s1.setSendDate(new Date());
            s1.setLongCode(longcode+i);
            s1.setMobile(mobile+2*i);
            s1.setCorpName(companies.get(i%5));
            s1.setSmsContent(SmsLogs.doc.substring((i-1)*100,i*100));
            s1.setState(i%2);
            s1.setOperatorId(i%3);
            s1.setProvince(provinces.get(i%4));
            s1.setIpAddr("127.0.0."+i);
            s1.setReplyTotal(i*3);
            s1.setFee(i*6+"");
            String json1  = mapper.writeValueAsString(s1);
            bulkRequest.add(new IndexRequest(index,type,s1.getId().toString()).source(json1, XContentType.JSON));
            System.out.println("数据"+i+s1.toString());

            BulkResponse responses = client.bulk(bulkRequest, RequestOptions.DEFAULT);
        }
    }


}
