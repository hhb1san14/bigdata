package com.hhb.hadoop.mapreduce.partition;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author: huanghongbo
 * @Date: 2020-07-05 16:07
 * @Description: 需求：
 * 按照不同的appkey把记录输出到不同的分区中
 * <p>
 * 001 001577c3 kar_890809 120.196.100.99 1116 954 200
 * 日志id 设备id appkey(合作硬件厂商) 网络ip 自有内容时长(秒) 第三方内 容时长(秒) 网络状态码
 */
public class CustomPartition extends Partitioner<Text, PartitionBean> {
    @Override
    public int getPartition(Text text, PartitionBean partitionBean, int numPartitions) {
        if (text.toString().equals("kar")) {
            return 1;
        } else if (text.toString().equals("pandora")) {
            return 2;
        } else {
            return 0;
        }
    }
}
