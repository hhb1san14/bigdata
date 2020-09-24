package com.hhb.hadoop.mapreduce.snappy;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author: huanghongbo
 * @Date: 2020-07-04 19:27
 * @Description: dirver类
 * 1. 获取配置文件对象，获取job对象实例
 * 2. 指定程序jar的本地路径
 * 3. 指定Mapper/Reducer类
 * 4. 指定Mapper输出的kv数据类型
 * 5. 指定最终输出的kv数据类型
 * 6. 指定job处理的原始数据路径
 * 7. 指定job输出结果路径
 * 8. 提交作业
 */
public class WordCountDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1. 获取配置文件对象，获取job对象实例
        Configuration conf = new Configuration();
        //设置reduce阶段的压缩
        conf.set("mapreduce.output.fileoutputformat.compress", "true");
        conf.set("mapreduce.output.fileoutputformat.compress.type", "RECORD");
        conf.set("mapreduce.output.fileoutputformat.compress.codec", "org.apache.hadoop.io.compress.SnappyCodec");
        Job job = Job.getInstance(conf, "WordCountDriver");
        //2. 指定程序jar的本地路径
        job.setJarByClass(WordCountDriver.class);
        //3. 指定Mapper/Reducer类
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        //4. 指定Mapper输出的kv数据类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //5. 指定最终输出的kv数据类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //6. 指定job处理的原始数据路径 /Users/baiwang/Desktop/hhb.txt
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        //7. 指定job输出结果路径 /Users/baiwang/Desktop/hhb.txt
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        //8. 提交作业
        boolean flag = job.waitForCompletion(true);
        //0：表示JVM正常退出，1：表示JVM异常退出
        System.exit(flag ? 0 : 1);
    }

}
