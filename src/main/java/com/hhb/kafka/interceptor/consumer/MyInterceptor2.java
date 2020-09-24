package com.hhb.kafka.interceptor.consumer;

import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;

/**
 * @description:
 * @author: huanghongbo
 * @date: 2020-08-18 19:33
 **/
public class MyInterceptor2 implements ConsumerInterceptor<String, String> {
    /**
     * poll方法返回结果之前，最后要调用的方法
     *
     * @param consumerRecords
     * @return
     */
    @Override
    public ConsumerRecords<String, String> onConsume(ConsumerRecords<String, String> consumerRecords) {

        System.err.println("2   --------     开始");

        //在这里消息不做处理，直接返回
        return consumerRecords;
    }

    /**
     * 消费者提交偏移量的时候，经过该方法
     *
     * @param map
     */
    @Override
    public void onCommit(Map<TopicPartition, OffsetAndMetadata> map) {
        System.err.println("2  ----------  结束");

    }

    /**
     * 用户关闭该拦截器用到的资源
     */
    @Override
    public void close() {

    }

    /**
     * 获取消费者的配置
     *
     * @param map
     */
    @Override
    public void configure(Map<String, ?> map) {

    }
}
