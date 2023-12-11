package cn.itcast.hotel;

import cn.itcast.hotel.pojo.HotelDoc;
import com.alibaba.fastjson.JSON;
import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.util.CollectionUtils;

import java.io.IOException;
import java.util.Map;

@SpringBootTest
public class HotelSearchTest {

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
    public void testMatchAll() throws IOException {
        // 1：准备Request
        SearchRequest request = new SearchRequest("hotel");
        // 2：准备DSL
        request.source().query(QueryBuilders.matchAllQuery());
        // 3：发送请求
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        // 4：解析搜索结果
        parseResponse(response);
    }


    @Test
    public void testMatch() throws IOException {
        // 1：准备Request
        SearchRequest request = new SearchRequest("hotel");
        // 2：准备DSL：全文检索查询：match查询
        request.source().query(QueryBuilders.matchQuery("all", "如家"));
        // 3：发送请求
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        // 4：解析搜索结果
        parseResponse(response);
    }

    private void parseResponse(SearchResponse response) {
        // 4：解析搜索结果
        SearchHits searchHits = response.getHits();
        // 4.1：获取总条数
        long total = searchHits.getTotalHits().value;
        System.out.println("共搜索到" + total + "条数据");
        // 4.2：文档数组
        SearchHit[] hits = searchHits.getHits();
        // 4.3：遍历
        for (SearchHit hit : hits) {
            // 4.4：获取文档source
            String json = hit.getSourceAsString();
            // 4.5：反序列化
            HotelDoc hotelDoc = JSON.parseObject(json, HotelDoc.class);

            // 4.6：高亮结果解析
            // 4.6.1：获取高亮结果
            Map<String, HighlightField> highlightFields = hit.getHighlightFields();
            if (!CollectionUtils.isEmpty(highlightFields)) {
                // 4.6.2：根据字段获取高亮结果
                HighlightField nameHighlight = highlightFields.get("name");
                if (nameHighlight != null) {
                    // 4.6.3：获取高亮值
                    String name = nameHighlight.getFragments()[0].string();
                    // 4.6.1：覆盖非高亮结果
                    hotelDoc.setName(name);
                }
                HighlightField brandHighlight = highlightFields.get("brand");
                if (brandHighlight != null) {
                    String brand = brandHighlight.getFragments()[0].toString();
                    hotelDoc.setBrand(brand);
                }
            }
            System.out.println("hotelDoc = " + hotelDoc);
        }
    }

    @Test
    public void testMultiMatch() throws IOException {
        // 1：准备Request
        SearchRequest request = new SearchRequest("hotel");
        // 2：准备DSL：全文检索查询：match查询
        request.source().query(QueryBuilders.multiMatchQuery("外滩如家", "name", "brand"));
        // 3：发送请求
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        // 4：解析搜索结果
        parseResponse(response);
    }

    @Test
    public void testBoolean() throws IOException {
        SearchRequest request = new SearchRequest("hotel");
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        boolQuery.must(QueryBuilders.termQuery("city", "上海"));
        boolQuery
                .should(QueryBuilders.termQuery("brand", "皇冠假日"))
                .should(QueryBuilders.termQuery("brand", "华美达"));
        boolQuery.mustNot(QueryBuilders.rangeQuery("price").lte(500));
        boolQuery.filter(QueryBuilders.rangeQuery("score").gte(45));
        request.source().query(boolQuery);
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        parseResponse(response);
    }


    @Test
    public void testSortAndPage() throws IOException {
        SearchRequest request = new SearchRequest("hotel");
        request.source().query(QueryBuilders.matchAllQuery());
        request.source().sort("price", SortOrder.ASC)
                .sort("score", SortOrder.DESC);
        request.source().from(0).size(5);
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        parseResponse(response);
    }

    @Test
    public void tesstHighlight() throws IOException {
        SearchRequest request = new SearchRequest("hotel");
        request.source().query(QueryBuilders.matchQuery("all", "上海如家"));
        request.source().highlighter(new HighlightBuilder()
                .field("name").field("brand")
                .requireFieldMatch(false)
                .preTags("<h1>")
                .postTags("</h1>")
        );
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        parseResponse(response);
    }
}
