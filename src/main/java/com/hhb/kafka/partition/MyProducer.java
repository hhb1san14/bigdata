package com.hhb.kafka.partition;

import com.hhb.kafka.serialize.UserSerializer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashMap;
import java.util.Map;

/**
 * @description:
 * @author: huanghongbo
 * @date: 2020-08-13 20:44
 **/
public class MyProducer {


    public static void main(String[] args) {

        Map<String, Object> map = new HashMap<>();
        map.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hhb:9092");
        map.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        map.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        //指定自定义分区器
        map.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, MyPartitioner.class);


        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(map);

        //不要设置指定的分区
        ProducerRecord<String, String> record = new ProducerRecord<String, String>(
                "topic_partition_01",
                "myKey",
                "myValue"
        );

        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                System.err.println("输出分区信息：" + recordMetadata.partition());
                System.err.println("输出主题信息：" + recordMetadata.topic());
                System.err.println("输出偏移量信息：" + recordMetadata.offset());
            }
        });

        producer.close();


    }

}
