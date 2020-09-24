package com.hhb.kafka.offset;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.*;
import java.util.function.BiConsumer;

/**
 * @description:
 * @author: huanghongbo
 * @date: 2020-08-17 21:38
 **/
public class MyOffsetManager {


    public static void main(String[] args) {


        Map<String, Object> map = new HashMap<>();
        map.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hhb:9092");
        map.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        map.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        map.put(ConsumerConfig.GROUP_ID_CONFIG, "myGroup1");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(map);
        //给消费者组里面的消费者分配分区，懒加载，只有调用poll方法的时候，才会真正的把分区分配给消费者
//        consumer.subscribe(Collections.singleton("tp_demo_01"));
        //如何手动给消费者分配分区

        //1. 需要知道哪些主题可以访问、消费
        //返回Map<主题，List<主题的分区>>
        Map<String, List<PartitionInfo>> stringListMap = consumer.listTopics();
        stringListMap.forEach((topicName, partitionInfos) -> {
            System.err.println("当前主题：" + topicName);
            for (PartitionInfo partitionInfo : partitionInfos) {
                System.err.println("分区信息： " + partitionInfo.toString());
            }
            System.err.println("============");
        });
        System.err.println("============");

        //获取给当前消费者分配的主题分区信息
        Set<TopicPartition> assignment = consumer.assignment();
        System.err.println("打印分配之前的信息：" + assignment);
        System.err.println("============");
        //给当前消费者分配主题和分区
        consumer.assign(Arrays.asList(
                new TopicPartition("tp_demo_01", 0),
                new TopicPartition("tp_demo_01", 1),
                new TopicPartition("tp_demo_01", 2)
        ));

        //获取给当前消费者分配的主题分区信息
        Set<TopicPartition> assignment1 = consumer.assignment();
        System.err.println("打印分配之后的信息：" + assignment1);
        System.err.println("============");

        //查看消费者在tp_demo_01 主题 0号分区的偏移量
        long offset = consumer.position(new TopicPartition("tp_demo_01", 0));
        System.err.println("查看消费者在tp_demo_01 主题 0号分区的位移：" + offset);
        System.err.println("============");

        //移动偏移量到开始的位置
        consumer.seekToBeginning(Arrays.asList(
                new TopicPartition("tp_demo_01", 0),
                new TopicPartition("tp_demo_01", 2)
        ));
        System.err.println("查看消费者在tp_demo_01 主题 0号分区的位移：" + consumer.position(new TopicPartition("tp_demo_01", 0)));
        System.err.println("查看消费者在tp_demo_01 主题 2号分区的位移：" + consumer.position(new TopicPartition("tp_demo_01", 2)));
        System.err.println("============");
        //移动偏移量到末尾的位置
        consumer.seekToEnd(Arrays.asList(
                new TopicPartition("tp_demo_01", 0),
                new TopicPartition("tp_demo_01", 2)
        ));
        System.err.println("查看消费者在tp_demo_01 主题 0号分区的位移：" + consumer.position(new TopicPartition("tp_demo_01", 0)));
        System.err.println("查看消费者在tp_demo_01 主题 2号分区的位移：" + consumer.position(new TopicPartition("tp_demo_01", 2)));

        System.err.println("============");
        //移动偏移量到具体位置：
        consumer.seek(new TopicPartition("tp_demo_01", 0), 15);
        System.err.println("查看消费者在tp_demo_01 主题 0号分区的位移：" + consumer.position(new TopicPartition("tp_demo_01", 0)));

        consumer.close();
    }


}
