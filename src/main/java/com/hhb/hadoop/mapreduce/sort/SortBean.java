package com.hhb.hadoop.mapreduce.sort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author: huanghongbo
 * @Date: 2020-07-06 12:50
 * @Description:
 */
public class SortBean implements WritableComparable<SortBean> {


    private String id;

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

    @Override
    public int compareTo(SortBean o) {
        if (sumDuration > o.getSumDuration()) {
            return -1;
        } else if (sumDuration == o.getSumDuration()) { // 当加入第二个判断条件的时候，就是二次排序
            return 0;
        } else {
            return 1;
        }

    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(id);
        dataOutput.writeLong(selfDuration);
        dataOutput.writeLong(thirdPartDuration);
        dataOutput.writeLong(sumDuration);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.id = dataInput.readUTF();
        this.selfDuration = dataInput.readLong();
        thirdPartDuration = dataInput.readLong();
        sumDuration = dataInput.readLong();
    }


    public String getId() {
        return id;
    }

    public SortBean setId(String id) {
        this.id = id;
        return this;
    }

    public long getSelfDuration() {
        return selfDuration;
    }

    public SortBean setSelfDuration(long selfDuration) {
        this.selfDuration = selfDuration;
        return this;
    }

    public long getThirdPartDuration() {
        return thirdPartDuration;
    }

    public SortBean setThirdPartDuration(long thirdPartDuration) {
        this.thirdPartDuration = thirdPartDuration;
        return this;
    }

    public long getSumDuration() {
        return sumDuration;
    }

    public SortBean setSumDuration(long sumDuration) {
        this.sumDuration = sumDuration;
        return this;
    }

    @Override
    public String toString() {
        return id + "\t" + selfDuration + "\t" + thirdPartDuration + "\t" + sumDuration;
    }
}
