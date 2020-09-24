package com.hhb.hadoop.mapreduce.sequence.input;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author: huanghongbo
 * @Date: 2020-07-06 19:36
 * @Description:
 */
public class SequenceMapper extends Mapper<Text, BytesWritable, Text, BytesWritable> {

    @Override
    protected void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {
        // 读取内容直接输出
        context.write(key, value);


    }
}
