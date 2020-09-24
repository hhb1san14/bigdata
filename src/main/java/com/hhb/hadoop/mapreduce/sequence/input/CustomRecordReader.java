package com.hhb.hadoop.mapreduce.sequence.input;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @author: huanghongbo
 * @Date: 2020-07-06 19:23
 * @Description: 负责读取数据，一次将整个文本数据读取完，封装成kv输出
 */
public class CustomRecordReader extends RecordReader<Text, BytesWritable> {


    private FileSplit fileSplit;

    private Configuration configuration;

    private Text key = new Text();

    private BytesWritable value = new BytesWritable();

    //用来判断是否读取过文件的表示
    private boolean flag = true;

    /**
     * 初始化数据，把切片和上下文提升为全局属性
     *
     * @param split
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        fileSplit = (FileSplit) split;
        configuration = context.getConfiguration();
    }

    /**
     * 读取数据
     *
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (flag) {
            //创建一个字节数组，长度为文件的大小
            byte[] bytes = new byte[(int) fileSplit.getLength()];
            //获取文件路径
            Path path = fileSplit.getPath();
            //获取文件信息
            FileSystem fileSystem = path.getFileSystem(configuration);
            //获取输入流
            FSDataInputStream fsDataInputStream = fileSystem.open(path);
            //将输入流的数组复制到bytes中
            IOUtils.readFully(fsDataInputStream, bytes, 0, bytes.length);
            key.set(fileSplit.getPath().toString());
            value.set(bytes, 0, bytes.length);
            flag = false;
            fsDataInputStream.close();
            return true;
        }
        return false;
    }

    /**
     * 获取当前的key
     *
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    /**
     * 获取进度
     *
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public void close() throws IOException {

    }
}
