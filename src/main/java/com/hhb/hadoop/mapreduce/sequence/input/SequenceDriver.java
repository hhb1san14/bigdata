package com.hhb.hadoop.mapreduce.sequence.input;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author: huanghongbo
 * @Date: 2020-07-06 19:40
 * @Description:
 */
public class SequenceDriver {


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1. 获取配置文件对象，获取job对象实例
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "SequenceDriver");
        //2. 指定程序jar的本地路径
        job.setJarByClass(SequenceDriver.class);
        //3. 指定Mapper/Reducer类
        job.setMapperClass(SequenceMapper.class);
        job.setReducerClass(SequenceReducer.class);
        //4. 指定Mapper输出的kv数据类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(BytesWritable.class);
        //5. 指定最终输出的kv数据类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);

        //设置读取文件的方式
        job.setInputFormatClass(CustomInputFormat.class);

        //6. 指定job处理的原始数据路径 /Users/baiwang/Desktop/hhb.txt
        FileInputFormat.setInputPaths(job, new Path("/Users/baiwang/myproject/hadoop/src/main/data/小文件"));
        //7. 指定job输出结果路径 /Users/baiwang/Desktop/hhb.txt
        FileOutputFormat.setOutputPath(job, new Path("/Users/baiwang/myproject/hadoop/src/main/data/小文件/seq/输出"));
        //8. 提交作业
        boolean flag = job.waitForCompletion(true);
        //0：表示JVM正常退出，1：表示JVM异常退出
        System.exit(flag ? 0 : 1);
    }


}
