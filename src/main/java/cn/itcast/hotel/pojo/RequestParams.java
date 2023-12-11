package cn.itcast.hotel.pojo;

import lombok.Data;

/**
 * @author hyh
 */
@Data
public class RequestParams {
    private String key;
    private Integer page;
    private Integer size;
    private String orderBy;

    private String brand;
    private String city;
    // 酒店星级
    private String starName;
    private Integer minPrice;
    private Integer maxPrice;

    // 经纬度
    private String location;
}
