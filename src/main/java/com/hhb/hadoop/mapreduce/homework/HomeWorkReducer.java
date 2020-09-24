package com.hhb.hadoop.mapreduce.homework;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


/**
 * @author: huanghongbo
 * @Date: 2020-07-06 21:13
 * @Description:
 */
public class HomeWorkReducer extends Reducer<HomeWork, NullWritable, HomeWork, NullWritable> {

    private long index = 0;

    @Override
    protected void reduce(HomeWork key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        index++;
        for (NullWritable value : values) {
            key.setIndex(index);
            context.write(key, NullWritable.get());
        }
    }
}
