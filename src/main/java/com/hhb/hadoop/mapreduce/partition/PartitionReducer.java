package com.hhb.hadoop.mapreduce.partition;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author: huanghongbo
 * @Date: 2020-07-05 16:07
 * @Description: 需求：
 * 按照不同的appkey把记录输出到不同的分区中
 * <p>
 * 001 001577c3 kar_890809 120.196.100.99 1116 954 200
 * 日志id 设备id appkey(合作硬件厂商) 网络ip 自有内容时长(秒) 第三方内 容时长(秒) 网络状态码
 */
public class PartitionReducer extends Reducer<Text, PartitionBean, Text, PartitionBean> {


    @Override
    protected void reduce(Text key, Iterable<PartitionBean> values, Context context) throws IOException, InterruptedException {
        for (PartitionBean value : values) {
            context.write(key, value);
        }
    }
}
