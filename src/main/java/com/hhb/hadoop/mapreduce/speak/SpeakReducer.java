package com.hhb.hadoop.mapreduce.speak;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author: huanghongbo
 * @Date: 2020-07-05 10:54
 * @Description:
 */
public class SpeakReducer extends Reducer<Text, SpeakBean, Text, SpeakBean> {

    private SpeakBean speakBean = new SpeakBean();

    @Override
    protected void reduce(Text key, Iterable<SpeakBean> values, Context context) throws IOException, InterruptedException {

        long selfDuration = 0L;
        long thirdPartDuration = 0L;
        for (SpeakBean speakBean : values) {
            selfDuration += speakBean.getSelfDuration();
            thirdPartDuration += speakBean.getThirdPartDuration();
        }
        speakBean.setSelfDuration(selfDuration);
        speakBean.setThirdPartDuration(thirdPartDuration);
        speakBean.setSumDuration(selfDuration + thirdPartDuration);
        context.write(key, speakBean);
    }
}
