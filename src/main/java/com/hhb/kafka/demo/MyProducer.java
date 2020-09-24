package com.hhb.kafka.demo;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * @description:
 * @author: huanghongbo
 * @date: 2020-08-12 11:34
 **/
public class MyProducer {

    @Test
    public void test() {
        Map<String, Object> configs = new HashMap<>();
        //初始链接
        configs.put("bootstrap.servers", "10.112.33.24:9092,10.112.33.67:9092,10.112.33.43:9092");
        //key的序列化类
        configs.put("key.serializer", IntegerSerializer.class);
        //value的序列化类
        configs.put("value.serializer", StringSerializer.class);
        //创建一个生产者
        KafkaProducer<Integer, String> producer = new KafkaProducer<>(configs);

        //自定义用户消息头字段
//        List<Header> list = new ArrayList<>();
//        list.add(new RecordHeader("biz.name", "producer.demo".getBytes()));
//        //组装 ProducerRecord
        ProducerRecord<Integer, String> producerRecord = new ProducerRecord<Integer, String>(
                "topic_1",
                0,
                0,
                "hello world2!"
        );
        //消息同步确认
//        Future<RecordMetadata> send = producer.send(producerRecord);
//        RecordMetadata recordMetadata = send.get();
//        System.err.println("输出分区信息：" + recordMetadata.partition());
//        System.err.println("输出主题信息：" + recordMetadata.topic());
//        System.err.println("输出偏移量信息：" + recordMetadata.offset());
        //消息异步确认
        producer.send(producerRecord, (RecordMetadata recordMetadata, Exception e) -> {
            if (e != null) {
                System.err.println("异常消息： " + e.getMessage());
                return;
            }
            System.err.println("输出分区信息：" + recordMetadata.partition());
            System.err.println("输出主题信息：" + recordMetadata.topic());
            System.err.println("输出偏移量信息：" + recordMetadata.offset());
        });
        //关闭生产者
        producer.close();
    }


}
