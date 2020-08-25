package com.wen.demo;

import com.wen.utils.ESClient;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.junit.Test;

import java.io.IOException;

public class Demo2 {
    RestHighLevelClient client = ESClient.getRestHighLevelClient();
    String index = "person";
    String type="man";


    /**
     * 创建索引
     * @throws IOException
     */
    @Test
    public void createIndex ()throws IOException {

        Settings.Builder put = Settings.builder().put("number_of_shards", "5").put("number_of_replicas", 1);
        XContentBuilder mappings = JsonXContent.contentBuilder()
                .startObject()
                .startObject("properties")
                .startObject("name")
                .field("type", "text")
                .endObject()
                .startObject("age")
                .field("type", "integer")
                .endObject()
                .startObject("birthday")
                .field("type", "date")
                .field("format", "yyyy-MM-dd || epoch_millis")
                .endObject()
                .endObject()
                .endObject();


        CreateIndexRequest request = new CreateIndexRequest(index).settings(put).mapping(type,mappings);
        CreateIndexResponse response = client.indices().create(request, RequestOptions.DEFAULT);
        System.out.println("response:"+response.toString());

    }


    /**
     * 索引是否存在
     * @throws IOException
     */
    @Test
    public void exists()throws IOException{
        GetIndexRequest request = new GetIndexRequest();
        request.indices(index);
        boolean exists = client.indices().exists(request, RequestOptions.DEFAULT);
        System.out.println(exists);

    }

    /**
     * 删除索引
     * @throws IOException
     */
    @Test
    public void del() throws IOException{
        DeleteIndexRequest request = new DeleteIndexRequest();
        request.indices(index);
        AcknowledgedResponse delete = client.indices().delete(request, RequestOptions.DEFAULT);
        System.out.println(delete.isAcknowledged());
    }


}
