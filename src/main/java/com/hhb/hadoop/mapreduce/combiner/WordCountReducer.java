package com.hhb.hadoop.mapreduce.combiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 需求：单词计数Map操作
 *
 * @author: huanghongbo
 * @Date: 2020-07-04 18:48
 * @Description: 操作步骤
 * 1、继承Reducer。
 * 2、定义泛型，两个key、value
 * 第一对key value为map的输出数据的类型
 * 第二对就是reduce的输出数据的数据类型
 * 3、重写reduce方法
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable total = new IntWritable();

    /**
     * 重新reduce方法，这里就是统计单词出现过几次
     *
     * @param key     map的结果的key
     * @param values  map的结果的key对应的value组成的额迭代器
     * @param context
     * @throws IOException
     * @throws InterruptedException
     * @Description ： 如果map输出为hello 1，hello 1，hello 1，reduce 1，hadoop1，reduce 1
     * 则：
     * key：hello，values:<1,1,1>
     * key：reduce，values:<1,1>
     * key：hadoop1，values:<1>
     * <p>
     * reduce 被调用的次数：3次
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        total.set(sum);
        context.write(key, total);
    }
}
