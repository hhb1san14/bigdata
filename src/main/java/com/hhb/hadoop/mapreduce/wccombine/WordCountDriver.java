package com.hhb.hadoop.mapreduce.wccombine;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
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

    /**
     * 不指定CombineInputFarmat
     * 输出日志：输入4个文件，4个切片
     * 2020-07-06 17:35:57,541 INFO [org.apache.hadoop.mapreduce.lib.input.FileInputFormat] - Total input files to process : 4
     * 2020-07-06 17:35:57,608 INFO [org.apache.hadoop.mapreduce.JobSubmitter] - number of splits:4
     *
     * @param args
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1. 获取配置文件对象，获取job对象实例
        Configuration conf = new Configuration();
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
        // 设置完使用CombineInputFormat读取数据
        //日志输出：输入的文件是4，但是切片数是3
        //2020-07-06 17:41:57,277 INFO [org.apache.hadoop.mapreduce.lib.input.FileInputFormat] - Total input files to process : 4
        //2020-07-06 17:41:57,299 INFO [org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat] - DEBUG: Terminated node allocation with : CompletedNodes: 1, size left: 2457600
        //2020-07-06 17:41:57,356 INFO [org.apache.hadoop.mapreduce.JobSubmitter] - number of splits:3
        // 设置使用CombineInputFormat读取数据
        job.setInputFormatClass(CombineTextInputFormat.class);
        // 设置虚拟存储切片的最大值4M（可以自己设置，单位是B）
        CombineTextInputFormat.setMaxInputSplitSize(job, 5194304);
        //6. 指定job处理的原始数据路径 /Users/baiwang/Desktop/hhb.txt
        FileInputFormat.setInputPaths(job, new Path("/Users/baiwang/myproject/hadoop/src/main/data/小文件"));
        //7. 指定job输出结果路径 /Users/baiwang/Desktop/hhb.txt
        FileOutputFormat.setOutputPath(job, new Path("/Users/baiwang/myproject/hadoop/src/main/data/小文件/输出"));
        //8. 提交作业
        boolean flag = job.waitForCompletion(true);
        //0：表示JVM正常退出，1：表示JVM异常退出
        System.exit(flag ? 0 : 1);
    }

}
