package com.hhb.kafka.serialize;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashMap;
import java.util.Map;

/**
 * @description:
 * @author: huanghongbo
 * @date: 2020-08-13 20:21
 **/
public class MyProducer {


    public static void main(String[] args) {

        Map<String, Object> map = new HashMap<>();
        map.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hhb:9092");
        map.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        //设置自定义的序列化器
        map.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, UserSerializer.class);
        KafkaProducer<String, User> producer = new KafkaProducer<String, User>(map);
        User user = new User();
//        user.setUserId(113).setUserName("李四");
        user.setUserId(113).setUserName("张三阿凡达");
        ProducerRecord<String, User> record = new ProducerRecord<String, User>(
                "topic_user_1", //主题
                user.getUserName(), //key
                user // value

        );
        producer.send(record, (recordMetadata, e) -> {
            if (e != null) {
                System.err.println("发送失败");
            }
            System.err.println("输出分区信息：" + recordMetadata.partition());
            System.err.println("输出主题信息：" + recordMetadata.topic());
            System.err.println("输出偏移量信息：" + recordMetadata.offset());
        });

        producer.close();

    }


}
