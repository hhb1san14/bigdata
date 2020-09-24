package com.hhb.kafka.demo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @description:
 * @author: huanghongbo
 * @date: 2020-08-12 15:48
 **/
public class MyConsumer {


    @Test
    public void test() {


        Map<String, Object> config = new HashMap<>();
        //服务器地址
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.112.33.24:9092,10.112.33.67:9092,10.112.33.43:9092");
        //配置key的反序列化类
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        //配置value的反序列化类
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        //消费组
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer_demo");
        //如果找不到消费者有效的偏移量，则自动重置到开始，earliest表示最早的偏移量
        //latest表示直接重置到消息偏移的最后一个
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");


        KafkaConsumer<Integer, String> consumer = new KafkaConsumer<>(config);

        List<String> topics = new ArrayList<>();
        topics.add("topic_1");
        //先订阅，在消费
        consumer.subscribe(topics);
        //拉取记录
        while (true) {
            //
            //批量从主题拉取消息,如果拉取不到数据，等待3秒再去拉取数据，
            ConsumerRecords<Integer, String> consumerRecords = consumer.poll(3_000);
            //遍历本次从主题的分区拉取的批量消息
            consumerRecords.forEach((ConsumerRecord<Integer, String> consumerRecord) -> {
                System.err.println("消费者：分区：" + consumerRecord.partition() +
                        "，主题：" + consumerRecord.topic() +
                        ",提交偏移量:" + consumerRecord.offset() +
                        ",key :  " + consumerRecord.key() +
                        ",value: " + consumerRecord.value());
            });
        }


    }

}
