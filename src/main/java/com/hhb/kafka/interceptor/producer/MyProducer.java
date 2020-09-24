package com.hhb.kafka.interceptor.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.IntegerSerializer;
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
        map.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        map.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        //设置拦截器，如果设置多个拦截器，则填写多个拦截器的全限定类名，中间用逗号隔开
        map.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, "com.hhb.kafka.interceptor.MyInterceptor1,com.hhb.kafka.interceptor.MyInterceptor2,com.hhb.kafka.interceptor.MyInterceptor3");

        //测试使用的配置
        map.put("testConfig", "this is test config");

        KafkaProducer<Integer, String> producer = new KafkaProducer<Integer, String>(map);

        //不要设置指定的分区
        ProducerRecord<Integer, String> record = new ProducerRecord<Integer, String>(
                "topic_interception_01",
                0,
                123,
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
