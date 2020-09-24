package com.hhb.hadoop.mapreduce.group;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @author: huanghongbo
 * @Date: 2020-07-06 14:12
 * @Description:
 */
public class CustomGroupingComparator extends WritableComparator {

    public CustomGroupingComparator() {
        super(GroupBean.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        GroupBean a1 = (GroupBean) a;
        GroupBean b1 = (GroupBean) b;
        return a1.getId().compareTo(b1.getId());
    }
}
