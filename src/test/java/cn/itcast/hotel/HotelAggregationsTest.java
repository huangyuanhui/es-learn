package cn.itcast.hotel;

import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.List;

@SpringBootTest
public class HotelAggregationsTest {

    private RestHighLevelClient client;

    @BeforeEach
    public void setup() {
        this.client = new RestHighLevelClient(RestClient.builder(
                HttpHost.create("http://120.77.168.189:9200")
        ));
    }

    @AfterEach
    public void teardown() throws IOException {
        this.client.close();
    }

    @Test
    public void testAggregation() throws IOException {
        // 准备请求
        SearchRequest request = new SearchRequest("hotel");
        // 准备DSL
        request.source().size(0);
        request.source().aggregation(
                AggregationBuilders
                        .terms("brandAgg")
                        .field("brand")
                        .size(15)
        );
        // 发送请求
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        // 结果解析
        Aggregations aggregations = response.getAggregations();
        // 根据聚合名称获取聚合结果
        Terms brandTerms = aggregations.get("brandAgg");
        // 获取buckets
        List<? extends Terms.Bucket> brandBuckets = brandTerms.getBuckets();
        // 遍历
        for (Terms.Bucket bucket : brandBuckets) {
            String brandKey = bucket.getKeyAsString();
            System.out.println("brandKey = " + brandKey);
        }
    }



}
