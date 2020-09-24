package com.hhb.hadoop.mapreduce.speak;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author: huanghongbo
 * @Date: 2020-07-05 10:58
 * @Description:
 */
public class SpeakDriver {


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 1. 获取配置文件对象，获取job对象实例
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, "SpeakDriver");
        // 2. 指定程序jar的本地路径
        job.setJarByClass(SpeakDriver.class);
        // 3. 指定Mapper/Reducer类
        job.setMapperClass(SpeakMapper.class);
        job.setReducerClass(SpeakReducer.class);
        // 4. 指定Mapper输出的kv数据类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(SpeakBean.class);
        // 5. 指定最终输出的kv数据类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(SpeakBean.class);
        // 6. 指定job处理的原始数据路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        // 7. 指定job输出结果路径
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        // 8. 提交作业
        boolean flag = job.waitForCompletion(true);
        System.exit(flag ? 0 : 1);

    }

}
