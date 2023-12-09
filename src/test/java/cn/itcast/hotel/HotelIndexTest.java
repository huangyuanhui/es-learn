package cn.itcast.hotel;

import cn.itcast.hotel.constants.HotelConstants;
import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;

/**
 * RestClient操作ElasticSearch的索引库
 */
@SpringBootTest
public class HotelIndexTest {

    private RestHighLevelClient client;

    @BeforeEach
    public void setUp() {
        this.client = new RestHighLevelClient(RestClient.builder(
                HttpHost.create("http://120.77.168.189:9200")
        ));
    }

    @AfterEach
    public void tearDown() throws IOException {
        this.client.close();
    }

    @Test
    public void  testInit() {
        System.out.println("client = " + client);
    }

    @Test
    public void testCreateIndex() throws IOException {
        // 1：创建索引请求对象
        CreateIndexRequest request = new CreateIndexRequest("hotel");
        // 2：DSL语句
        request.source(HotelConstants.MAPPING_TEMPLATE, XContentType.JSON);
        // 3：发送请求创建索引
        client.indices().create(request, RequestOptions.DEFAULT);
    }

    @Test
    public void testDeleteIndex() throws IOException {
        DeleteIndexRequest request = new DeleteIndexRequest("hotel");
        client.indices().delete(request, RequestOptions.DEFAULT);
    }

    @Test
    public void testExistsIndex() throws IOException {
        GetIndexRequest request = new GetIndexRequest("hotel");
        boolean exists = client.indices().exists(request, RequestOptions.DEFAULT);
        System.out.println("exists = " + exists);
    }
}
