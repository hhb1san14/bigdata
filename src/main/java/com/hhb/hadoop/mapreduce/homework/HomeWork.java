package com.hhb.hadoop.mapreduce.homework;


import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author: huanghongbo
 * @Date: 2020-07-06 21:07
 * @Description:
 */
public class HomeWork implements WritableComparable<HomeWork> {


    private long index;

    private long number;

    public long getIndex() {
        return index;
    }

    public HomeWork setIndex(long index) {
        this.index = index;
        return this;
    }

    public long getNumber() {
        return number;
    }

    public HomeWork setNumber(long number) {
        this.number = number;
        return this;
    }

    @Override
    public int compareTo(HomeWork o) {
        if (number > o.getNumber()) {
            return 1;
        }
        return -1;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(index);
        dataOutput.writeLong(number);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        index = dataInput.readLong();
        number = dataInput.readLong();
    }

    @Override
    public String toString() {
        return index + "\t" + number;
    }
}
