package com.hhb.kafka.interceptor.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * @description:
 * @author: huanghongbo
 * @date: 2020-08-18 19:25
 **/
public class MyConsumer {

    public static void main(String[] args) {

        Map<String, Object> configs = new HashMap<>();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hhb:9092");
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, "mygrp");
        configs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        configs.put(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG,
                "com.hhb.kafka.interceptor.consumer.MyInterceptor1,com.hhb.kafka.interceptor.consumer.MyInterceptor2,com.hhb.kafka.interceptor.consumer.MyInterceptor3");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(configs);
        consumer.subscribe(Collections.singleton("tp_demo_01"));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(5000);
            records.forEach(record -> {
                System.err.println("消费者：分区：" + record.partition() +
                        "，主题：" + record.topic() +
                        ",提交偏移量:" + record.offset() +
                        ",key :  " + record.key() +
                        ",value: " + record.value());
            });
        }
    }


}
