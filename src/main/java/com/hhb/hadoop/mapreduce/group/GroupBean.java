package com.hhb.hadoop.mapreduce.group;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author: huanghongbo
 * @Date: 2020-07-06 13:53
 * @Description:
 */
public class GroupBean implements WritableComparable<GroupBean> {

    private String id;

    private Double price;

    public String getId() {
        return id;
    }

    public GroupBean setId(String id) {
        this.id = id;
        return this;
    }

    public Double getPrice() {
        return price;
    }

    public GroupBean setPrice(Double price) {
        this.price = price;
        return this;
    }

    @Override
    public int compareTo(GroupBean o) {
        //如果不对订单排序，使用GroupingComparator的时候，是两两比较。这样相邻的两个
        //di不同，无法分到一个组。
//        if (price > o.getPrice()) {
//            return -1;
//        } else if (price < o.getPrice()) {
//            return 1;
//        }
//        return 0;
        int result = id.compareTo(o.getId());
        if (result == 0) {
            result = -price.compareTo(o.getPrice());
        }
        return result;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(id);
        dataOutput.writeDouble(price);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        id = dataInput.readUTF();
        price = dataInput.readDouble();
    }

    @Override
    public String toString() {
        return id + "\t" + price;
    }
}
