package com.hhb.hadoop.mapreduce.sort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author: huanghongbo
 * @Date: 2020-07-06 12:52
 * @Description:
 */
public class SortMapper extends Mapper<LongWritable, Text, SortBean, NullWritable> {


    private SortBean sortBean = new SortBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] fileds = value.toString().split("\t");
        sortBean.setId(fileds[0]);
        sortBean.setSelfDuration(Long.parseLong(fileds[1]));
        sortBean.setThirdPartDuration(Long.parseLong(fileds[2]));
        sortBean.setSumDuration(Long.parseLong(fileds[3]));

        context.write(sortBean, NullWritable.get());
    }
}
