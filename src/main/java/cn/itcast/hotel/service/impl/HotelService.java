package cn.itcast.hotel.service.impl;

import cn.itcast.hotel.mapper.HotelMapper;
import cn.itcast.hotel.pojo.Hotel;
import cn.itcast.hotel.pojo.HotelDoc;
import cn.itcast.hotel.pojo.PageResult;
import cn.itcast.hotel.pojo.RequestParams;
import cn.itcast.hotel.service.IHotelService;
import com.alibaba.fastjson.JSON;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Service
public class HotelService extends ServiceImpl<HotelMapper, Hotel> implements IHotelService {

    @Autowired
    private RestHighLevelClient client;

    @Override
    public PageResult search(RequestParams params) {
        try {
            SearchRequest request = new SearchRequest("hotel");

            // query：查询DSL
            QueryBuilder boolQuery = buildBasicQuery(params);
            request.source().query(boolQuery);

            // from+size：分页
            int page = params.getPage();
            int size = params.getSize();
            request.source().from((page - 1) * size).size(size);

            // 排序
            String location = params.getLocation();
            if (!StringUtils.isEmpty(location)) {
                request.source().sort(SortBuilders
                        .geoDistanceSort("location", new GeoPoint(location))
                        .order(SortOrder.ASC)
                        .unit(DistanceUnit.KILOMETERS)
                );
            }

            // 发送请求
            SearchResponse response = client.search(request, RequestOptions.DEFAULT);
            // 解析响应
            return parseResponse(response);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 组装查询条件
     *
     * @param params
     * @return
     */
    private QueryBuilder buildBasicQuery(RequestParams params) {
        // 查询
        BoolQueryBuilder boolQuery = new BoolQueryBuilder();
        // 关键字搜索
        String searchKey = params.getKey();
        if (StringUtils.isEmpty(searchKey)) {
            boolQuery.must(QueryBuilders.matchAllQuery());
        } else {
            boolQuery.must(QueryBuilders.matchQuery("all", searchKey));
        }
        // 条件过滤-城市city
        String city = params.getCity();
        if (!StringUtils.isEmpty(city)) {
            boolQuery.filter(QueryBuilders.termQuery("city", city));
        }
        // 条件过滤-品牌brand
        String brand = params.getBrand();
        if (!StringUtils.isEmpty(brand)) {
            boolQuery.filter(QueryBuilders.termQuery("brand", brand));
        }
        // 条件过滤-星级starName
        String starName = params.getStarName();
        if (!StringUtils.isEmpty(starName)) {
            boolQuery.filter(QueryBuilders.termQuery("starName", starName + "级"));
        }
        // 条件过滤-价格price
        if (params.getMinPrice() != null && params.getMaxPrice() != null) {
            int minPrice = params.getMinPrice();
            int maxPrice = params.getMaxPrice();
            boolQuery.filter(QueryBuilders
                    .rangeQuery("price").gte(minPrice).lte(maxPrice));
        }

        // 算分函数-function score
        FunctionScoreQueryBuilder functionScoreQuery =
                QueryBuilders.functionScoreQuery(
                        // 原始查询
                        boolQuery,
                        // function score的数组
                        new FunctionScoreQueryBuilder.FilterFunctionBuilder[]{
                                // 其中的一个function score元素
                                new FunctionScoreQueryBuilder.FilterFunctionBuilder(
                                        /// 过滤条件
                                        QueryBuilders.termQuery("isAD", true),
                                        // 算分函数
                                        ScoreFunctionBuilders.weightFactorFunction(10)
                                )
                        });
        return functionScoreQuery;
    }

    /**
     * 搜索结果解析
     *
     * @param response
     */
    private PageResult parseResponse(SearchResponse response) {
        // 4：解析搜索结果
        SearchHits searchHits = response.getHits();
        // 4.1：获取总条数
        long total = searchHits.getTotalHits().value;
        // 4.2：文档数组
        SearchHit[] hits = searchHits.getHits();
        // 4.3：遍历
        List<HotelDoc> hotels = new ArrayList<>();
        for (SearchHit hit : hits) {
            // 4.4：获取文档source
            String json = hit.getSourceAsString();
            // 4.5：反序列化
            HotelDoc hotelDoc = JSON.parseObject(json, HotelDoc.class);
            // 4.6：获取排序值
            Object[] sortValues = hit.getSortValues();
            if (sortValues.length > 0) {
                Object sortValue = sortValues[0];
                hotelDoc.setDistance(sortValue);
            }
            hotels.add(hotelDoc);
        }
        return new PageResult(total, hotels);
    }
}
