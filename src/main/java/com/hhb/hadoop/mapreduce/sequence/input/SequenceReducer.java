package com.hhb.hadoop.mapreduce.sequence.input;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author: huanghongbo
 * @Date: 2020-07-06 19:38
 * @Description:
 */
public class SequenceReducer extends Reducer<Text, BytesWritable, Text, BytesWritable> {

    @Override
    protected void reduce(Text key, Iterable<BytesWritable> values, Context context) throws IOException, InterruptedException {
        //values 的size应该是1，只有一个文件，直接输出第一个文件就好
        context.write(key, values.iterator().next());
    }
}
