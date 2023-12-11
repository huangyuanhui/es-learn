package cn.itcast.hotel.controller;

import cn.itcast.hotel.pojo.PageResult;
import cn.itcast.hotel.pojo.RequestParams;
import cn.itcast.hotel.service.IHotelService;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;

@RequestMapping("/hotel")
@RestController
public class HotelController {

    @Resource
    private IHotelService hotelService;

    @PostMapping("/list")
    public PageResult search(@RequestBody RequestParams params) {
        return hotelService.search(params);
    }
}
