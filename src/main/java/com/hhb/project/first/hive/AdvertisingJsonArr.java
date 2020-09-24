package com.hhb.project.first.hive;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Strings;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * @description:
 * @author: huanghongbo
 * @date: 2020-09-07 18:24
 **/
public class AdvertisingJsonArr extends UDF {


    public List<String> evaluate(String jsonStr) {

        if (Strings.isNullOrEmpty(jsonStr)) {
            return null;
        }
        try {
            List<String> list = new ArrayList<>();
            JSONArray jsonArray = JSONObject.parseArray(jsonStr);
            for (int i = 0; i < jsonArray.size(); i++) {
                Object o = jsonArray.get(i);
                list.add(JSONObject.toJSONString(o));
            }
            return list;
        } catch (Exception e) {
            return null;
        }
    }


    @Test
    public void test() {
        String str = "[{\"name\":\"goods_detail_loading\",\"json\":{\"entry\":\"3\",\"goodsid\":\"0\",\"loading_time\":\"21\",\"action\":\"3\",\"staytime\":\"83\",\"showtype\":\"2\"},\"time\":1595338801959},{\"name\":\"loading\",\"json\":{\"loading_time\":\"12\",\"action\":\"3\",\"loading_type\":\"3\",\"type\":\"3\"},\"time\":1595343257273},{\"name\":\"notification\",\"json\":{\"action\":\"1\",\"type\":\"3\"},\"time\":1595278347546},{\"name\":\"ad\",\"json\":{\"duration\":\"11\",\"ad_action\":\"0\",\"shop_id\":\"9\",\"event_type\":\"ad\",\"ad_type\":\"4\",\"show_style\":\"0\",\"product_id\":\"4\",\"place\":\"placecampaign2_left\",\"sort\":\"6\"},\"time\":1595331392660},{\"name\":\"favorites\",\"json\":{\"course_id\":6,\"id\":0,\"userid\":0},\"time\":1595280136738},{\"name\":\"praise\",\"json\":{\"id\":4,\"type\":1,\"add_time\":\"1597848120187\",\"userid\":9,\"target\":5},\"time\":1595286969963}]";
        List<String> evaluate = evaluate(str);
        System.err.println(evaluate);
    }


}
