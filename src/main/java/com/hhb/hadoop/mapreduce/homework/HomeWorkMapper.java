package com.hhb.hadoop.mapreduce.homework;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author: huanghongbo
 * @Date: 2020-07-06 21:05
 * @Description:
 */
public class HomeWorkMapper extends Mapper<LongWritable, Text, HomeWork, NullWritable> {


    private HomeWork homeWork = new HomeWork();

    /**
     * 结果输出
     *
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        homeWork.setNumber(Long.parseLong(value.toString()));
        context.write(homeWork,NullWritable.get());
    }
}
