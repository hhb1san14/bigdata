package com.hhb.hadoop.mapreduce.sequence.input;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;


/**
 * @author: huanghongbo
 * @Date: 2020-07-06 19:21
 * @Description:
 */
public class CustomInputFormat extends FileInputFormat<Text, BytesWritable> {


    /**
     * 判断文件是否需要切片，当前操作是一次读取整个文件，不需要切片
     *
     * @param context
     * @param filename
     * @return
     * @throws IOException
     */
    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return false;
    }

    /**
     * 创建自定义的RecordReader，用来读取数据
     *
     * @param split
     * @param context
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public RecordReader<Text, BytesWritable> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        CustomRecordReader customRecordReader = new CustomRecordReader();
        customRecordReader.initialize(split, context);
        return customRecordReader;
    }
}
