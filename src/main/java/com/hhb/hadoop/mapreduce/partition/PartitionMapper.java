package com.hhb.hadoop.mapreduce.partition;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author: huanghongbo
 * @Date: 2020-07-05 16:07
 * @Description: 需求：
 * * 按照不同的appkey把记录输出到不同的分区中
 * * <p>
 * * 001 001577c3 kar_890809 120.196.100.99 1116 954 200
 * * 日志id 设备id appkey(合作硬件厂商) 网络ip 自有内容时长(秒) 第三方内 容时长(秒) 网络状态码
 */
public class PartitionMapper extends Mapper<LongWritable, Text, Text, PartitionBean> {


    private PartitionBean bean = new PartitionBean();

    private Text text = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fileds = value.toString().split("\t");
        bean.setId(fileds[0]);
        bean.setDeviceId(fileds[1]);
        bean.setAppKey(fileds[2]);
        bean.setIp(fileds[3]);
        bean.setSelfDuration(fileds[4]);
        bean.setThirdPartDuration(fileds[5]);
        text.set(fileds[2]);
        context.write(text, bean);
    }
}
