package com.hhb.kafka.admin;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * @description:
 * @author: huanghongbo
 * @date: 2020-08-18 21:35
 **/
public class MyAdminClient {


    private KafkaAdminClient kafkaAdminClient;


    /**
     * 初始化KafkaAdminClient客户端
     */
    @Before
    public void init() {
        HashMap<String, Object> configs = new HashMap<>();
        configs.put("bootstrap.servers", "hhb:9092");


        kafkaAdminClient = (KafkaAdminClient) KafkaAdminClient.create(configs);
    }


    /**
     * 新建一个主题
     *
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @Test
    public void testCreateTopics() throws ExecutionException, InterruptedException {
        //创建一个hhb 的topic，3个分区，1个副本
        NewTopic newTopic = new NewTopic("hhb", 3, (short) 1);
        CreateTopicsResult topics = kafkaAdminClient.createTopics(Collections.singleton(newTopic));
        Void aVoid = topics.all().get();
        System.err.println(aVoid);
        Map<String, KafkaFuture<Void>> values = topics.values();
        System.err.println(values);


    }

    /**
     * 列出所有的主题
     *
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @Test
    public void testListTopics() throws ExecutionException, InterruptedException {

        ListTopicsOptions listTopicsOptions = new ListTopicsOptions();
        //是否获取内置主题
        listTopicsOptions.listInternal(true);
        //请求超时时间，毫秒
        listTopicsOptions.timeoutMs(500);
        //列出所有的主题
        ListTopicsResult listTopicsResult = kafkaAdminClient.listTopics(listTopicsOptions);
        //列出所有的非内置主题
        //ListTopicsResult listTopicsResult = kafkaAdminClient.listTopics();
        //打印所有的主题名称
        Set<String> strings = listTopicsResult.names().get();
        for (String name : strings) {
            System.err.println("name==>>" + name);
        }
        System.err.println("========================================================================================================");
        //将请求变成同步的，直接获取结果
        Collection<TopicListing> topicListings = listTopicsResult.listings().get();
        topicListings.forEach(topics -> {
            //是否是一个内置的主题，内置的主题：_consumer_offsets_
            boolean internal = topics.isInternal();
            //主题名称
            String name = topics.name();
            System.err.println("是否为内部主题：" + internal + ",该主题的名字： " + name + ", toString" + topics.toString());
        });
        System.err.println("========================================================================================================");
    }


    /**
     * 关闭KafkaAdminClient客户端
     */
    @After
    public void destroy() {
        kafkaAdminClient.close();
    }
}
