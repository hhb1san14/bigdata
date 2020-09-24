package com.hhb.hadoop.mapreduce.speak;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author: huanghongbo
 * @Date: 2020-07-05 10:37
 * @Description:
 */
public class SpeakMapper extends Mapper<LongWritable, Text, Text, SpeakBean> {


    private Text k = new Text();

    private SpeakBean speakBean = new SpeakBean();

    /**
     * 文门格式
     * 01	a00df6s	kar	120.196.100.99	384	33	200
     * 日志id 设备id appkey(合作硬件厂商) 网络ip 自有内容时长(秒) 第三方内 容时长(秒) 网络状态码
     *
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //取出每行的数据
        String[] fileds = value.toString().split("\t");
        Long selfDuration = Long.valueOf(fileds[fileds.length - 3]);
        Long thirdPartDuration = Long.valueOf(fileds[fileds.length - 2]);
        speakBean.setSelfDuration(selfDuration);
        speakBean.setThirdPartDuration(thirdPartDuration);
        speakBean.setSumDuration(selfDuration + thirdPartDuration);
        k.set(fileds[1]);
        context.write(k, speakBean);
    }
}
