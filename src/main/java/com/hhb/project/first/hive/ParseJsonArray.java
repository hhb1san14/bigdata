package com.hhb.project.first.hive;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Strings;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * @description:
 * @author: huanghongbo
 * @date: 2020-09-03 15:26
 **/
public class ParseJsonArray extends UDF {


    public ArrayList<String> evaluate(String jsonStr, String key) {
        if (Strings.isNullOrEmpty(jsonStr)) {
            return null;
        }
        try {
            ArrayList<String> list = new ArrayList<>();
            for (Object o : JSONObject.parseObject(jsonStr).getJSONArray(key)) {
                list.add(o.toString());
            }
            return list;
        } catch (Exception e) {
            return null;
        }
    }


    @Test
    public void testParseJsonArray() {
        String jsonStr = "{\"id\": 1,\"ids\": [101,102,103],\"total_number\": 3}";
        String key = "ids";
        List list = evaluate(jsonStr, key);
        System.err.println(JSONObject.toJSONString(list));
    }


}
