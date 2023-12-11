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
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
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
            // 查询
            String searchKey = params.getKey();
            if (StringUtils.isEmpty(searchKey)) {
                request.source().query(QueryBuilders.matchAllQuery());
            } else {
                request.source().query(QueryBuilders.matchQuery("all", searchKey));
            }
            // 分页
            int page = params.getPage();
            int size = params.getSize();
            request.source().from((page - 1) * size).size(size);

            // 发送请求
            SearchResponse response = client.search(request, RequestOptions.DEFAULT);
            // 解析响应
            return parseResponse(response);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
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
            hotels.add(hotelDoc);
        }
        return new PageResult(total, hotels);
    }
}
