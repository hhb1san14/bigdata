package com.hhb.kafka.tx.single;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @description:
 * @author: huanghongbo
 * @date: 2020-08-23 20:37
 **/
public class ProducerDemo {

    //    public static void main(String[] args) {
//        Map<String, Object> config = new HashMap<>();
//        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hhb:9092");
//        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
//        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
//
//        //设置客户端ID
//        config.put(ProducerConfig.CLIENT_ID_CONFIG, "tx_producer");
//
//        //设置事务ID
//        config.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "my_tx_id");
//
//        //需要所有的ISR确认，才算提交
//        config.put(ProducerConfig.ACKS_CONFIG, "all");
//
//        //生产者
//        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(config);
//        //初始化事务
//        producer.initTransactions();
//
//        //开启事务
//        producer.beginTransaction();
//
//        try {
//            producer.send(new ProducerRecord<>("tp_tx_01", "k1", "v1"));
//            producer.send(new ProducerRecord<>("tp_tx_01", "k2", "v2"));
//            producer.send(new ProducerRecord<>("tp_tx_01", "k3", "v3"));
//
////            int i = 1 / 0;
//
//        } catch (Exception e) {
//            System.err.println("出现异常");
//            producer.abortTransaction();
//        } finally {
//            producer.close();
//        }
//
//
//    }
//    public static void main(String[] args) throws ExecutionException,
//            InterruptedException {
//        Properties props = new Properties();
//        // 客户端id
//        props.put("client.id", "KafkaProducerDemo");
//        // kafka地址,列表格式为host1:port1,host2:port2,...，无需添加所有的集群地址，kafka会根据提供的地址发现其他的地址(建议多提供几个，以防提供的服务器关闭)
//        props.put("bootstrap.servers", "localhost:9092");
//        // 发送返回应答方式
//        // 0:Producer 往集群发送数据不需要等到集群的返回，不确保消息发送成功。安全性最 低但是效率最高。
//        // 1:Producer 往集群发送数据只要 Leader 应答就可以发送下一条，只确保Leader接收成功。
//        // -1或者all:Producer 往集群发送数据需要所有的ISR Follower都完成从Leader 的同步才会发送下一条，确保Leader发送成功和所有的副本都成功接收。安全性最高，但是效率最低。
//        props.put("acks", "all");
//        // 重试次数
//        props.put("retries", 0);
//        // 重试间隔时间
//        props.put("retries.backoff.ms", 100);
//        // 批量发送的大小
//        props.put("batch.size", 16384);
//        // 一个Batch被创建之后，最多过多久，不管这个Batch有没有写满，都必须发送出去
//        props.put("linger.ms", 10);
//        // 缓冲区大小
//        props.put("buffer.memory", 33554432);
//        // key序列化方式
//        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//        // value序列化方式
//        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//        // topic
//        String topic = "lagou_edu";
//        Producer<String, String> producer = new KafkaProducer<>(props);
//        AtomicInteger count = new AtomicInteger();
//        while (true) {
//            int num = count.get();
//            String key = Integer.toString(num);
//            String value = Integer.toString(num);
//            ProducerRecord<String, String> record = new ProducerRecord<>
//                    (topic, key, value);
//            if (num % 2 == 0) {
//                // 偶数异步发送
//                // 第一个参数record封装了topic、key、value
//                // 第二个参数是一个callback对象，当生产者接收到kafka发来的ACK确认消息时，会调用此CallBack对象的onComplete方法
//                producer.send(record, (recordMetadata, e) -> {
//                    System.out.println("num:" + num + " topic:" + recordMetadata.topic() + " offset:" + recordMetadata.offset());
//                });
//            } else {
//                // 同步发送
//                // KafkaProducer.send方法返回的类型是Future<RecordMetadata>，通 过get方法阻塞当前线程，等待kafka服务端ACK响应
//                producer.send(record).get();
//            }
//            count.incrementAndGet();
//            TimeUnit.MILLISECONDS.sleep(100);
//        }
//    }

    public static void main(String[] args) throws InterruptedException {
        // 是否自动提交
        Boolean autoCommit = false;
        // 是否异步提交
        Boolean isSync = true;
        Properties props = new Properties();
        // kafka地址,列表格式为host1:port1,host2:port2,...，无需添加所有的集群地址， kafka会根据提供的地址发现其他的地址(建议多提供几个，以防提供的服务器关闭)
        props.put("bootstrap.servers", "localhost:9092");
        // 消费组
        props.put("group.id", "test");
        // 开启自动提交offset
        props.put("enable.auto.commit", autoCommit.toString());
        // 1s自动提交
        props.put("auto.commit.interval.ms", "1000");
        // 消费者和群组协调器的最大心跳时间，如果超过该时间则认为该消费者已经死亡或者故 障，需要踢出消费者组
        props.put("session.timeout.ms", "60000");
        // 一次poll间隔最大时间
        props.put("max.poll.interval.ms", "1000");
        // 当消费者读取偏移量无效的情况下，需要重置消费起始位置，默认为latest(从消费者启动后生成的记录)，另外一个选项值是 earliest，将从有效的最小位移位置开始消费
        props.put("auto.offset.reset", "latest");
        // consumer端一次拉取数据的最大字节数
        props.put("fetch.max.bytes", "1024000");
        // key序列化方式
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // value序列化方式
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>
                (props);
        String topic = "lagou_edu";
        // 订阅topic列表
        consumer.subscribe(Arrays.asList(topic));
        while (true) {
            // 消息拉取
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
            }
            if (!autoCommit) {
                if (isSync) {
                    // 处理完成单次消息以后，提交当前的offset，如果失败会一直重试直至成功
                    consumer.commitSync();
                } else {
                    // 异步提交
                    consumer.commitAsync((offsets, exception) -> {
                        exception.printStackTrace();
                        System.out.println(offsets.size());
                    });
                }
            }
            TimeUnit.SECONDS.sleep(3);
        }
    }


}