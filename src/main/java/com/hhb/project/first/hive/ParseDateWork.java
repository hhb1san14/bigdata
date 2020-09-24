package com.hhb.project.first.hive;

import com.google.common.base.Strings;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

/**
 * @description:
 * @author: huanghongbo
 * @date: 2020-09-17 20:08
 **/
public class ParseDateWork extends UDF {


    /**
     * 传入一个日期，要求格式为：yyyy-MM-dd
     *
     * @param dateStr
     * @return
     */
    public String evaluate(String dateStr) {
        if (Strings.isNullOrEmpty(dateStr)) {
            return "3";
        }
        return request(dateStr);
    }

    /**
     * 调用互联网上已有的接口，也可以自己穷举
     *
     * @param httpArg
     * @return
     */
    public String request(String httpArg) {
        String httpUrl = "http://tool.bitefu.net/jiari/";
        BufferedReader reader = null;
        String result = null;
        StringBuffer sbf = new StringBuffer();
        httpUrl = httpUrl + "?d=" + httpArg;
        try {
            URL url = new URL(httpUrl);
            HttpURLConnection connection = (HttpURLConnection) url
                    .openConnection();
            connection.setRequestMethod("GET");
            connection.connect();
            InputStream is = connection.getInputStream();
            reader = new BufferedReader(new InputStreamReader(is, "UTF-8"));
            String strRead = null;
            while ((strRead = reader.readLine()) != null) {
                sbf.append(strRead);
            }
            reader.close();
            result = sbf.toString();
        } catch (Exception e) {
            e.printStackTrace();
            return "3";
        }
        return result;
    }

}
