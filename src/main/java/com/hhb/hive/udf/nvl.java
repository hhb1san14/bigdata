package com.hhb.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * @author: huanghongbo
 * @Date: 2020-07-12 17:10
 * @Description:
 */
public class nvl extends UDF {

    public Text evaluate(Text x, Text y) {
        if (x == null || x.toString().trim().length() == 0) {
            return y;
        }
        return x;
    }


}
