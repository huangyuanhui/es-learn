package cn.itcast.hotel;

import cn.itcast.hotel.pojo.Hotel;
import cn.itcast.hotel.pojo.HotelDoc;
import cn.itcast.hotel.service.IHotelService;
import com.alibaba.fastjson.JSON;
import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.List;

@SpringBootTest
public class HotelDocumentTest {

    private RestHighLevelClient client;

    @Resource
    private IHotelService hotelService;

    @BeforeEach
    public void setup() {
        client = new RestHighLevelClient(RestClient.builder(
                HttpHost.create("http://120.77.168.189:9200")
        ));
    }

    @AfterEach
    public void tearDown() throws IOException {
        client.close();
    }

    @Test
    public void testAddDocument() throws IOException {
        // 0：从数据库根据id获取酒店数据
        Hotel hotel = hotelService.getById(36934L);
        // 转换为文档类型
        HotelDoc hotelDoc = new HotelDoc(hotel);
        // 1：准备请求对象
        IndexRequest request = new IndexRequest("hotel").id(hotelDoc.getId().toString());
        // 2：装备JSON格式的DSL
        request.source(JSON.toJSONString(hotelDoc), XContentType.JSON);
        // 3：发送请求
        client.index(request, RequestOptions.DEFAULT);
    }


    @Test
    public void testGetDocumentById() throws IOException {
        GetRequest request = new GetRequest("hotel", "36934");
        GetResponse response = client.get(request, RequestOptions.DEFAULT);
        String jsonStr = response.getSourceAsString();
        HotelDoc hotelDoc = JSON.parseObject(jsonStr, HotelDoc.class);
        System.out.println("hotelDoc = " + hotelDoc);
    }

    @Test
    public void testUpdateDocumentById() throws IOException {
        UpdateRequest request = new UpdateRequest("hotel", "36934");
        request.doc(
                "price", "337",
                "starName", "三钻"
        );
        client.update(request, RequestOptions.DEFAULT);
    }


    @Test
    public void tesDeleteDocumentById() throws IOException {
        DeleteRequest request = new DeleteRequest("hotel", "36934");
        client.delete(request, RequestOptions.DEFAULT);
    }

    @Test
    public void testBulkRequest() throws IOException {
        List<Hotel> hotels = hotelService.list();
        BulkRequest request = new BulkRequest();
        hotels.forEach(hotel -> {
            HotelDoc hotelDoc = new HotelDoc(hotel);
            IndexRequest indexRequest = new IndexRequest("hotel")
                    .id(hotelDoc.getId().toString())
                    .source(JSON.toJSONString(hotelDoc), XContentType.JSON);
            request.add(indexRequest);
        });
        client.bulk(request, RequestOptions.DEFAULT);
    }
}
