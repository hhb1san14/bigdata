package com.hhb.kafka.deserialize;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * @description:
 * @author: huanghongbo
 * @date: 2020-08-17 19:57
 **/
public class UserConsumer {


    public static void main(String[] args) {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hhb:9092");
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        //设置自定义的反序列化器
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, UserDeserializer.class);

        configs.put(ConsumerConfig.GROUP_ID_CONFIG, "user_consumer");
        configs.put(ConsumerConfig.CLIENT_ID_CONFIG, "consumer_id");
        configs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // 设置偏移量为自动提交
        configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        // 偏移量自动提交的时间间隔
        configs.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "3000");

        KafkaConsumer<Integer, User> consumer = new KafkaConsumer<>(configs);

        consumer.subscribe(Collections.singleton("topic_user_1"));
        ConsumerRecords<Integer, User> records = consumer.poll(Long.MAX_VALUE);
        records.forEach(record -> System.err.println(record.value() + "    " + record.key()));
        consumer.close();
    }


}
