package com.hhb.hadoop.mapreduce.partition;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

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
public class PartitionDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        // 1. 获取配置文件对象，获取job对象实例
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, "PartitionDriver");
        // 2. 指定程序jar的本地路径
        job.setJarByClass(PartitionDriver.class);
        // 3. 指定Mapper/Reducer类
        job.setMapperClass(PartitionMapper.class);
        job.setReducerClass(PartitionReducer.class);
        // 4. 指定Mapper输出的kv数据类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(PartitionBean.class);
        // 5. 指定最终输出的kv数据类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(PartitionBean.class);
        // 6. 指定job处理的原始数据路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        // 7. 指定job输出结果路径
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        // 设置ReduceTask数量
        job.setNumReduceTasks(3);
//        job.setNumReduceTasks(2);
//        job.setNumReduceTasks(5);
        // 设置自定义分区
        job.setPartitionerClass(CustomPartition.class);
        // 8. 提交作业
        boolean flag = job.waitForCompletion(true);
        System.exit(flag ? 0 : 1);



    }
}
