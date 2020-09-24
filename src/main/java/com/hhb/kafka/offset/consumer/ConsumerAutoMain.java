package com.hhb.kafka.offset.consumer;

/**
 * @description:
 * @author: huanghongbo
 * @date: 2020-08-19 20:54
 **/
public class ConsumerAutoMain {
    public static void main(String[] args) {
        KafkaConsumerAuto kafka_consumerAuto = new KafkaConsumerAuto();
        try {
            kafka_consumerAuto.execute();
            Thread.sleep(20000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            kafka_consumerAuto.shutdown();
        }
    }
}
