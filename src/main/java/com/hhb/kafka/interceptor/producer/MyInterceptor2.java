package com.hhb.kafka.interceptor.producer;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * @description:
 * @author: huanghongbo
 * @date: 2020-08-13 21:32
 **/
public class MyInterceptor2 implements ProducerInterceptor<Integer, String> {


    private final static Logger logger = LoggerFactory.getLogger(MyInterceptor2.class);

    /**
     * 消息发送的时候，经过拦截器，调用该方法
     */
    @Override
    public ProducerRecord<Integer, String> onSend(ProducerRecord<Integer, String> record) {
        System.err.println("拦截器2 --- go");
        //要发送的消息
        String topic = record.topic();
        Integer key = record.key();
        String value = record.value();
        Integer partition = record.partition();
        Long timestamp = record.timestamp();
        Headers headers = record.headers();
        //拦截器拦下来之后的根据原来消息创建新的消息，此处没有做任何改动
        ProducerRecord<Integer, String> newRecord = new ProducerRecord<>(
                topic, partition, timestamp, key, value, headers
        );
        //传递新的消息
        return newRecord;
    }

    /**
     * 消息确认或异常的时候，调用该方法，该方法不应该实现多大的任务，会影响生产者性能
     *
     * @param recordMetadata
     * @param e
     */
    @Override
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {
        System.err.println("拦截器2 --- back");
    }

    @Override
    public void close() {

    }

    /**
     * 可以获取到生产的配置信息
     *
     * @param map
     */
    @Override
    public void configure(Map<String, ?> map) {

        Object testConfig = map.get("testConfig");
        System.err.println("获取到的testConfig值为：" + testConfig);
    }
}
