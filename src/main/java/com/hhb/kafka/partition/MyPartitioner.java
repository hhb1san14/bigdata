package com.hhb.kafka.partition;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * @description: 自定义分区器
 * @author: huanghongbo
 * @date: 2020-08-13 21:06
 **/
public class MyPartitioner implements Partitioner {

    /**
     * 为指定的消息记录计算分区值 *
     *
     * @param topic      主题名称
     * @param key        根据该key的值进行分区计算，如果没有则为null。
     * @param keyBytes   key的序列化字节数组，根据该数组进行分区计算。如果没有key，则为
     *                   null
     * @param value      根据value值进行分区计算，如果没有，则为null
     * @param valueBytes value的序列化字节数组，根据此值进行分区计算。如果没有，则为
     *                   null
     * @param cluster    当前集群的元数据
     */
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        //此处可以计算分区的数字
        //在这我们直接指定分区2
        return 2;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
