package com.hhb.hadoop.mapreduce.speak;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 输出结果：
 * <p>
 * 001577c3 11160 9540 20700
 * 设备id 自有内容时长(秒) 第三方内容时长(秒) 总时长
 *
 * @author: huanghongbo
 * @Date: 2020-07-05 10:38
 * @Description:
 */
public class SpeakBean implements Writable {


    /**
     * 自有时长内容
     */
    private long selfDuration;
    /**
     * 第三方时长内容
     */
    private long thirdPartDuration;
    /**
     * 总时长内容
     */
    private long sumDuration;

    public SpeakBean() {
    }

    public SpeakBean(long selfDuration, long thirdPartDuration) {
        this.selfDuration = selfDuration;
        this.thirdPartDuration = thirdPartDuration;
        this.sumDuration = selfDuration + thirdPartDuration;
    }

    public long getSelfDuration() {
        return selfDuration;
    }

    public SpeakBean setSelfDuration(long selfDuration) {
        this.selfDuration = selfDuration;
        return this;
    }

    public long getThirdPartDuration() {
        return thirdPartDuration;
    }

    public SpeakBean setThirdPartDuration(long thirdPartDuration) {
        this.thirdPartDuration = thirdPartDuration;
        return this;
    }

    public long getSumDuration() {
        return sumDuration;
    }

    public SpeakBean setSumDuration(long sumDuration) {
        this.sumDuration = sumDuration;
        return this;
    }

    /**
     * 重新序列化方法
     *
     * @param dataOutput
     * @throws IOException
     */
    @Override

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(selfDuration);
        dataOutput.writeLong(thirdPartDuration);
        dataOutput.writeLong(sumDuration);
    }

    /**
     * 重新反序列化方法
     *
     * @param dataInput
     * @throws IOException
     */
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        selfDuration = dataInput.readLong();
        thirdPartDuration = dataInput.readLong();
        sumDuration = dataInput.readLong();

    }

    @Override
    public String toString() {
        return selfDuration + "\t" + thirdPartDuration + "\t" + sumDuration;
    }
}
