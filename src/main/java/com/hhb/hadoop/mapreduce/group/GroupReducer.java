package com.hhb.hadoop.mapreduce.group;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author: huanghongbo
 * @Date: 2020-07-06 13:59
 * @Description:
 */
public class GroupReducer extends Reducer<GroupBean, NullWritable, GroupBean, NullWritable> {

    private int index = 0;

    @Override
    protected void reduce(GroupBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        index++;
        System.err.println("这是第： " + index + "次进入reduce");
        int i = 0;
        for (NullWritable value : values) {
            System.err.println(key.toString());
            if (i == 0) {
                context.write(key, NullWritable.get());
            }
            i++;
        }

    }


}
