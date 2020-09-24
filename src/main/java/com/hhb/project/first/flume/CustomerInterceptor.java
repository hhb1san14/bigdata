package com.hhb.project.first.flume;

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
public class CustomerInterceptor implements Interceptor {


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
            //将时间戳转换为字符串 "yyyy-MM-dd"
            String timeString = jsonObject.getJSONObject("app_active").getString("time");
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
            return new CustomerInterceptor();
        }

        @Override
        public void configure(Context context) {
        }
    }

    @Test
    public void test() {
        String str = "2020-08-20 11:56:00.365 [main] INFO  com.lagou.ecommerce.AppStart - {\"app_active\":{\"name\":\"app_active\",\"json\":{\"entry\":\"1\",\"action\":\"0\",\"error_code\":\"0\"},\"time\":1595266507583},\"attr\":{\"area\":\"菏泽\",\"uid\":\"2F10092A1\",\"app_v\":\"1.1.2\",\"event_type\":\"common\",\"device_id\":\"1FB872-9A1001\",\"os_type\":\"0.01\",\"channel\":\"YO\",\"language\":\"chinese\",\"brand\":\"Huawei-9\"}}";
        Event event = new SimpleEvent();
        event.setBody(str.getBytes());
        event.setHeaders(new HashMap<>());
        CustomerInterceptor testInterceptor = new CustomerInterceptor();
        testInterceptor.intercept(event);
        System.err.println(event.getHeaders());
    }
}
