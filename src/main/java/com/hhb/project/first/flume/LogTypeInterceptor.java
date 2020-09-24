package com.hhb.project.first.flume;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Charsets;
import com.google.common.base.Strings;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.interceptor.Interceptor;
import org.junit.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @description:
 * @author: huanghongbo
 * @date: 2020-09-01 19:18
 **/
public class LogTypeInterceptor implements Interceptor {


    @Override
    public void initialize() {

    }

    /**
     * 1. 获取 event 的 header
     * 2. 获取 event 的 body
     * 3. 解析body获取json串
     * 4. 解析json串获取时间戳
     * 5. 将时间戳转换为字符串 "yyyy-MM-dd"
     * 6. 将转换后的字符串放置header中
     * 7. 返回event
     *
     * @param event
     * @return
     */
    @Override
    public Event intercept(Event event) {
        // 获取 event 的 header
        Map<String, String> headers = event.getHeaders();
        // 获取 event 的 body
        String bodyStr = new String(event.getBody(), Charsets.UTF_8);
        // 解析body获取json串
        String[] bodyArr = bodyStr.split("\\s+");
        try {
            if (Strings.isNullOrEmpty(bodyArr[6])) {
                return null;
            }
            JSONObject jsonObject = JSONObject.parseObject(bodyArr[6]);
            String timeString = "";
            if ("start".equals(headers.get("logtype"))) {
                //将时间戳转换为字符串 "yyyy-MM-dd"
                timeString = jsonObject.getJSONObject("app_active").getString("time");
            } else if ("event".equals(headers.get("logtype"))) {
                JSONArray jsonArray = jsonObject.getJSONArray("lagou_event");
                if (jsonArray.size() > 0) {
                    timeString = jsonArray.getJSONObject(0).getString("time");
                }
            }

            long timeStamp = Long.parseLong(timeString);
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
            String date = formatter.format(LocalDateTime.ofInstant(Instant.ofEpochMilli(timeStamp), ZoneId.systemDefault()));
//            将转换后的字符串放置header中
            headers.put("logTime", date);
        } catch (Exception e) {
            headers.put("logTime", "unknown");
        }
        event.setHeaders(headers);
        return event;
    }

    @Override
    public List<Event> intercept(List<Event> list) {
        List<Event> out = new ArrayList<>();
        for (Event event : list) {
            Event outEvent = intercept(event);
            if (outEvent != null) {
                out.add(outEvent);
            }
        }
        return out;
    }

    @Override
    public void close() {

    }

    public static class Builder implements Interceptor.Builder {
        @Override
        public Interceptor build() {
            return new LogTypeInterceptor();
        }

        @Override
        public void configure(Context context) {
        }
    }

    @Test
    public void test() {
        String str = "2020-08-20 11:56:00.365 [main] INFO  com.lagou.ecommerce.AppStart - {\"app_active\":{\"name\":\"app_active\",\"json\":{\"entry\":\"1\",\"action\":\"0\",\"error_code\":\"0\"},\"time\":1595266507583},\"attr\":{\"area\":\"菏泽\",\"uid\":\"2F10092A1\",\"app_v\":\"1.1.2\",\"event_type\":\"common\",\"device_id\":\"1FB872-9A1001\",\"os_type\":\"0.01\",\"channel\":\"YO\",\"language\":\"chinese\",\"brand\":\"Huawei-9\"}}";
        Event event = new SimpleEvent();
        HashMap<String, String> map = new HashMap<>();
        map.put("logtype","start");

        event.setBody(str.getBytes());
        event.setHeaders(map);
        LogTypeInterceptor testInterceptor = new LogTypeInterceptor();
        testInterceptor.intercept(event);
        System.err.println(event.getHeaders());
    }


    @Test
    public void test2() {
        String str = "2020-08-20 12:00:30.111 [main] INFO  com.lagou.ecommerce.AppEvent - {\"lagou_event\":[{\"name\":\"goods_detail_loading\",\"json\":{\"entry\":\"2\",\"goodsid\":\"0\",\"loading_time\":\"107\",\"action\":\"3\",\"staytime\":\"108\",\"showtype\":\"5\"},\"time\":1595288680211},{\"name\":\"ad\",\"json\":{\"duration\":\"11\",\"ad_action\":\"0\",\"shop_id\":\"10\",\"event_type\":\"ad\",\"ad_type\":\"3\",\"show_style\":\"1\",\"product_id\":\"6\",\"place\":\"placecampaign1_right\",\"sort\":\"0\"},\"time\":1595271684867}],\"attr\":{\"area\":\"福州\",\"uid\":\"2F10092A2\",\"app_v\":\"1.1.0\",\"event_type\":\"common\",\"device_id\":\"1FB872-9A1002\",\"os_type\":\"5.79\",\"channel\":\"FD\",\"language\":\"chinese\",\"brand\":\"xiaomi-2\"}}";
        Event event = new SimpleEvent();
        HashMap<String, String> map = new HashMap<>();
        map.put("logtype","event");

        event.setBody(str.getBytes());
        event.setHeaders(map);
        LogTypeInterceptor testInterceptor = new LogTypeInterceptor();
        testInterceptor.intercept(event);
        System.err.println(event.getHeaders());
    }
}
