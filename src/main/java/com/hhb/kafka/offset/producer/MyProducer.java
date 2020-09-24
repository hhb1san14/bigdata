package com.hhb.kafka.offset.producer;

/**
 * @description:
 * @author: huanghongbo
 * @date: 2020-08-19 20:51
 **/
public class MyProducer {

    public static void main(String[] args) {
        Thread thread = new Thread(new ProducerHandler("hello lagou "));
        thread.start();
    }
}
