package com.hhb.hadoop.mapreduce.group;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author: huanghongbo
 * @Date: 2020-07-06 14:03
 * @Description:
 */
public class GroupDriver {


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, "GroupDriver");
        job.setJarByClass(GroupDriver.class);
        job.setMapperClass(GroupMapper.class);
        job.setReducerClass(GroupReducer.class);
        job.setMapOutputKeyClass(GroupBean.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(GroupBean.class);
        job.setOutputValueClass(NullWritable.class);
        // 设置分组
        job.setPartitionerClass(CustomGroupPartition.class);
        job.setNumReduceTasks(2);
        job.setGroupingComparatorClass(CustomGroupingComparator.class);
        FileInputFormat.setInputPaths(job, new Path("/Users/baiwang/myproject/hadoop/src/main/data/GroupingComparator"));
        FileOutputFormat.setOutputPath(job, new Path("/Users/baiwang/myproject/hadoop/src/main/data/GroupingComparator/out"));
        boolean flag = job.waitForCompletion(true);
        System.exit(flag ? 0 : 1);
    }
}
