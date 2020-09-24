package com.hhb.hadoop.mapreduce.wccombine;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 需求：单词计数Map操作
 *
 * @author: huanghongbo
 * @Date: 2020-07-04 18:29
 * @Description: 操作步骤
 * 1、继承Mapper
 * 2、定义泛型，
 * LongWritable, Text，输入参数的key，value，LongWritable为偏移量，Text一行文本，
 * Text, IntWritable ，输出单数的key，value，key为单词，value ：1 <单词，1>
 * 3、重写Mapper的map方法
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    //计数，输出的value
    private IntWritable one = new IntWritable(1);
    //单词，输出的key
    private Text word = new Text();


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String str = value.toString();
        String[] words = str.split(" ");
        for (String s : words) {
            word.set(s);
            context.write(word, one);
        }
    }
}