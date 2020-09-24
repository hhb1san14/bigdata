package com.hhb.hadoop.mapreduce.sequence.output;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author: huanghongbo
 * @Date: 2020-07-06 20:21
 * @Description:
 */
public class OutputDriver {


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1. 获取配置文件对象，获取job对象实例
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "OutputDriver");
        //2. 指定程序jar的本地路径
        job.setJarByClass(OutputDriver.class);
        //3. 指定Mapper/Reducer类
        job.setMapperClass(OutputMapper.class);
        job.setReducerClass(OutputReducer.class);
        //4. 指定Mapper输出的kv数据类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        //5. 指定最终输出的kv数据类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);


        job.setOutputFormatClass(CustomOutputFormat.class);


        //6. 指定job处理的原始数据路径 /Users/baiwang/Desktop/hhb.txt
        FileInputFormat.setInputPaths(job, new Path("/Users/baiwang/myproject/hadoop/src/main/data/click_log"));
        //7. 指定job输出结果路径 /Users/baiwang/Desktop/hhb.txt，因为输出的文件不仅仅有laguo.txt等，还有success文件
        FileOutputFormat.setOutputPath(job, new Path("/Users/baiwang/myproject/hadoop/src/main/data/click_log/out"));
        //8. 提交作业
        boolean flag = job.waitForCompletion(true);
        //0：表示JVM正常退出，1：表示JVM异常退出
        System.exit(flag ? 0 : 1);
    }
}
