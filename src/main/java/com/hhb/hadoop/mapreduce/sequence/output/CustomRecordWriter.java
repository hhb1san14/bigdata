package com.hhb.hadoop.mapreduce.sequence.output;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * @author: huanghongbo
 * @Date: 2020-07-06 20:11
 * @Description:
 */
public class CustomRecordWriter extends RecordWriter<Text, NullWritable> {

    private FSDataOutputStream lagou;

    private FSDataOutputStream other;

    //对两个输出流赋值
    public CustomRecordWriter(FSDataOutputStream lagou, FSDataOutputStream other) {
        this.lagou = lagou;
        this.other = other;
    }

    //写数据的路径
    @Override
    public void write(Text key, NullWritable value) throws IOException, InterruptedException {
        String line = key.toString();
        //如果一行包含lagou，则输出到lgou，否则输出到other
        if (line.contains("lagou")) {
            lagou.write(line.getBytes());
            lagou.write("\r\n".getBytes());
        } else {
            other.write(line.getBytes());
            other.write("\r\n".getBytes());
        }
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        IOUtils.closeStream(lagou);
        IOUtils.closeStream(other);
    }
}
