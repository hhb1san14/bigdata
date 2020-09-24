package com.hhb.kafka.tx.stream;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * @description:
 * @author: huanghongbo
 * @date: 2020-08-23 20:48
 **/
public class StreamDemo {


    public static KafkaProducer<String, String> getProducer() {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hhb:9092");
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        //设置客户端ID
        config.put(ProducerConfig.CLIENT_ID_CONFIG, "tx_producer");
        //设置事务ID
        config.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "my_tx_id");
        //需要所有的ISR确认，才算提交
        config.put(ProducerConfig.ACKS_CONFIG, "all");
        return new KafkaProducer<String, String>(config);
    }

    public static KafkaConsumer<String, String> getConsumer(String consumerGroupId) {
        Map<String, Object> config = new HashMap<>();

        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hhb:9092");
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        config.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);

        // 设置消费组ID
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer_grp_02");
        // 不启用消费者偏移量的自动确认，也不要手动确认
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return new KafkaConsumer<String, String>(config);

    }

    public static void main(String[] args) {
        String consumerGroupId = "consumer_grp_id_1";

        KafkaProducer<String, String> producer = getProducer();
        KafkaConsumer<String, String> consumer = getConsumer(consumerGroupId);

        //初始化事务
        producer.initTransactions();
        consumer.subscribe(Collections.singleton("tp_tx_01"));
        ConsumerRecords<String, String> records = consumer.poll(1000);
        //开启事务
        producer.beginTransaction();
        try {
            Map<TopicPartition, OffsetAndMetadata> metadataMap = new HashMap<>();
            for (ConsumerRecord<String, String> record : records) {
                System.err.println(record);
                producer.send(new ProducerRecord<String, String>("tp_tx_out_01", record.key(), record.value()));
                metadataMap.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset()));
            }
//            int i = 1 / 0;
            producer.sendOffsetsToTransaction(metadataMap, consumerGroupId);
            producer.commitTransaction();
        } catch (Exception e) {
            System.err.println("出现异常");
            producer.abortTransaction();
        } finally {
            producer.close();
            consumer.close();
        }


    }

    @Test
    public void test() {
        String dd = "console-consumer-90277";
        System.err.println(Math.abs(dd.hashCode()) % 50);
    }


}
