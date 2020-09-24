package com.hhb.hadoop.mapreduce.sequence.output;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author: huanghongbo
 * @Date: 2020-07-06 20:10
 * @Description:
 */
public class CustomOutputFormat extends FileOutputFormat<Text, NullWritable> {


    @Override
    public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        //获取配置信息
        Configuration configuration = context.getConfiguration();
        //获取文件系统高
        FileSystem fileSystem = FileSystem.get(configuration);
        //创建两个输出流
        FSDataOutputStream lagou = fileSystem.create(new Path("/Users/baiwang/myproject/hadoop/src/main/data/click_log/out/lagou.txt"));
        FSDataOutputStream other = fileSystem.create(new Path("/Users/baiwang/myproject/hadoop/src/main/data/click_log/out/other.txt"));
        CustomRecordWriter customRecordWriter = new CustomRecordWriter(lagou, other);
        return customRecordWriter;
    }
}
