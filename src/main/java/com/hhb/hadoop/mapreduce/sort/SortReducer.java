package com.hhb.hadoop.mapreduce.sort;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author: huanghongbo
 * @Date: 2020-07-06 12:57
 * @Description:
 */
public class SortReducer extends Reducer<SortBean, NullWritable, SortBean, NullWritable> {

    @Override
    protected void reduce(SortBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        //有可能存在总时长一样，导致合并成一个key，所以遍历values
        for (NullWritable value : values) {
            context.write(key, value);
        }
    }
}
