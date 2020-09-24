package com.hhb.hadoop.mapreduce.sort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author: huanghongbo
 * @Date: 2020-07-06 12:59
 * @Description:
 */
public class SortDriver {


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {


        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, "SortDriver");
        job.setJarByClass(SortDriver.class);
        job.setMapperClass(SortMapper.class);
        job.setReducerClass(SortReducer.class);
        job.setMapOutputKeyClass(SortBean.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(SortBean.class);
        job.setOutputValueClass(NullWritable.class);
        job.setNumReduceTasks(1);
        FileInputFormat.setInputPaths(job, new Path("/Users/baiwang/myproject/hadoop/src/main/data/mr-writable案例/输出"));
        FileOutputFormat.setOutputPath(job, new Path("/Users/baiwang/myproject/hadoop/src/main/data/mr-writable案例/输出/sortoutput"));
        boolean flag = job.waitForCompletion(true);
        System.exit(flag ? 0 : 1);

    }
}
