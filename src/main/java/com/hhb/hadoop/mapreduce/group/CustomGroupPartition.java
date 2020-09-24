package com.hhb.hadoop.mapreduce.group;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;


/**
 * @author: huanghongbo
 * @Date: 2020-07-06 14:07
 * @Description:
 */
public class CustomGroupPartition extends Partitioner<GroupBean, NullWritable> {

    @Override
    public int getPartition(GroupBean groupBean, NullWritable nullWritable, int numPartitions) {
        return (groupBean.getId().hashCode() & Integer.MAX_VALUE) % numPartitions;
    }
}
