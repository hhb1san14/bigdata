package com.hhb.hadoop.mapreduce.group;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


/**
 * @author: huanghongbo
 * @Date: 2020-07-06 13:56
 * @Description:
 */
public class GroupMapper extends Mapper<LongWritable, Text, GroupBean, NullWritable> {

    private GroupBean groupBean = new GroupBean();


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fileds = value.toString().split("\t");
        groupBean.setId(fileds[0]);
        groupBean.setPrice(Double.parseDouble(fileds[2]));
        context.write(groupBean, NullWritable.get());
    }
}
