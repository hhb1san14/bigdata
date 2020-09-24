package com.hhb.kafka.admin;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.requests.DescribeLogDirsResponse;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * @description:
 * @author: huanghongbo
 * @date: 2020-08-18 21:35
 **/
public class MyAdminClientTeacher {


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
    public void testCreateTopic() throws ExecutionException,
            InterruptedException {
        Map<String, String> configs = new HashMap<>();
        configs.put("max.message.bytes", "1048576");
        configs.put("segment.bytes", "1048576000");
        NewTopic newTopic = new NewTopic("hhb", 2, (short) 1);
        newTopic.configs(configs);
        CreateTopicsResult topics = kafkaAdminClient.createTopics(Collections.singleton(newTopic));
        KafkaFuture<Void> all = topics.all();
        Void aVoid = all.get();
        System.out.println(aVoid);
    }

    /**
     * 删除主题
     *
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @Test
    public void testDeleteTopic() throws ExecutionException,
            InterruptedException {
        DeleteTopicsOptions options = new DeleteTopicsOptions();
        options.timeoutMs(500);
        DeleteTopicsResult deleteResult = kafkaAdminClient.deleteTopics(Collections.singleton("hhb"), options);
        deleteResult.all().get();
    }

    /**
     * 修改配置信息
     *
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @Test
    public void testAlterTopic() throws ExecutionException, InterruptedException {
        NewPartitions newPartitions = NewPartitions.increaseTo(5);
        Map<String, NewPartitions> newPartitionsMap = new HashMap<>();
        newPartitionsMap.put("adm_tp_01", newPartitions);
        CreatePartitionsOptions option = new CreatePartitionsOptions();
        // Set to true if the request should be validated without creating new partitions.
        // 如果只是验证，而不创建分区，则设置为true
        // option.validateOnly(true);
        CreatePartitionsResult partitionsResult = kafkaAdminClient.createPartitions(newPartitionsMap, option);
        Void aVoid = partitionsResult.all().get();
    }

    /**
     * 查看topic详细信息
     *
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @Test
    public void testDescribeTopics() throws ExecutionException,
            InterruptedException {
        DescribeTopicsOptions options = new DescribeTopicsOptions();
        options.timeoutMs(3000);
        DescribeTopicsResult topicsResult = kafkaAdminClient.describeTopics(Collections.singleton("hhb"), options);
        Map<String, TopicDescription> stringTopicDescriptionMap = topicsResult.all().get();
        stringTopicDescriptionMap.forEach((k, v) -> {
            System.out.println(k + "\t" + v);
            System.out.println("=======================================");
            System.out.println(k);
            boolean internal = v.isInternal();
            String name = v.name();
            List<TopicPartitionInfo> partitions = v.partitions();
            String partitionStr = Arrays.toString(partitions.toArray());
            System.out.println("内部的?" + internal);
            System.out.println("topic name = " + name);
            System.out.println("分区:" + partitionStr);
            partitions.forEach(partition -> {
                System.out.println(partition);
            });
        });
    }

    /**
     * 查询集群信息
     *
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @Test
    public void testDescribeCluster() throws ExecutionException,
            InterruptedException {
        DescribeClusterResult describeClusterResult = kafkaAdminClient.describeCluster();
        KafkaFuture<String> stringKafkaFuture = describeClusterResult.clusterId();
        String s = stringKafkaFuture.get();
        System.out.println("cluster name = " + s);
        KafkaFuture<Node> controller = describeClusterResult.controller();
        Node node = controller.get();
        System.out.println("集群控制器:" + node);
        Collection<Node> nodes = describeClusterResult.nodes().get();
        nodes.forEach(node1 -> {
            System.out.println(node1);
        });
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
     * 查询配置信息
     *
     * @throws ExecutionException
     * @throws InterruptedException
     * @throws TimeoutException
     */
    @Test
    public void testDescribeConfigs() throws ExecutionException,
            InterruptedException, TimeoutException {
        ConfigResource configResource = new ConfigResource(ConfigResource.Type.BROKER, "0");
        DescribeConfigsResult describeConfigsResult =
                kafkaAdminClient.describeConfigs(Collections.singleton(configResource));
        Map<ConfigResource, Config> configMap =
                describeConfigsResult.all().get(15, TimeUnit.SECONDS);
        configMap.forEach(new BiConsumer<ConfigResource, Config>() {
            @Override
            public void accept(ConfigResource configResource, Config config) {
                ConfigResource.Type type = configResource.type();
                String name = configResource.name();
                System.out.println("资源名称:" + name);
                Collection<ConfigEntry> entries = config.entries();
                entries.forEach(new Consumer<ConfigEntry>() {
                    @Override
                    public void accept(ConfigEntry configEntry) {
                        boolean aDefault = configEntry.isDefault();
                        boolean readOnly = configEntry.isReadOnly();
                        boolean sensitive = configEntry.isSensitive();
                        String name1 = configEntry.name();
                        String value = configEntry.value();
                        System.out.println("是否默认:" + aDefault + "\t是否 只读?" + readOnly + "\t是否敏感?" + sensitive
                                + "\t" + name1 + " --> " + value);
                    }
                });
                ConfigEntry retries = config.get("retries");
                if (retries != null) {
                    System.out.println(retries.name() + " -->" +
                            retries.value());
                } else {
                    System.out.println("没有这个属性");
                }
            }

        });
    }

    @Test
    public void testAlterConfig() throws ExecutionException,
            InterruptedException {
        // 这里设置后，原来资源中不冲突的属性也会丢失，直接按照这里的配置设置
        Map<ConfigResource, Config> configMap = new HashMap<>();
        ConfigResource resource = new
                ConfigResource(ConfigResource.Type.TOPIC, "adm_tp_01");
        Config config = new Config(Collections.singleton(new
                ConfigEntry("segment.bytes", "1048576000")));
        configMap.put(resource, config);
        AlterConfigsResult alterConfigsResult = kafkaAdminClient.alterConfigs(configMap);
        Void aVoid = alterConfigsResult.all().get();
    }

    @Test
    public void testDescribeLogDirs() throws ExecutionException,
            InterruptedException {
        DescribeLogDirsOptions option = new DescribeLogDirsOptions();
        option.timeoutMs(1000);
        DescribeLogDirsResult describeLogDirsResult = kafkaAdminClient.describeLogDirs(Collections.singleton(0), option);
        Map<Integer, Map<String, DescribeLogDirsResponse.LogDirInfo>> integerMapMap
                = describeLogDirsResult.all().get();
        integerMapMap.forEach(new BiConsumer<Integer, Map<String, DescribeLogDirsResponse.LogDirInfo>>() {
            @Override
            public void accept(Integer integer, Map<String, DescribeLogDirsResponse.LogDirInfo> stringLogDirInfoMap) {
                System.out.println("broker.id = " + integer);
                stringLogDirInfoMap.forEach(new BiConsumer<String, DescribeLogDirsResponse.LogDirInfo>() {
                    @Override
                    public void accept(String s, DescribeLogDirsResponse.LogDirInfo logDirInfo) {
                        System.out.println("log.dirs:" + s);

                        // 查看该broker上的主题/分区/偏移量等信息 //
                        logDirInfo.replicaInfos.forEach(new BiConsumer<TopicPartition, DescribeLogDirsResponse.ReplicaInfo>() {
                            @Override
                            public void accept(TopicPartition topicPartition, DescribeLogDirsResponse.ReplicaInfo replicaInfo) {
                                int partition = topicPartition.partition();
                                String topic = topicPartition.topic();
                                boolean isFuture = replicaInfo.isFuture;
                                long offsetLag = replicaInfo.offsetLag;
                                long size = replicaInfo.size;
                                System.out.println("partition:" + partition + "\ttopic:" + topic + "\tisFuture:" + isFuture + "\toffsetLag:" + offsetLag + "\tsize:" + size);
                            }
                        });
                    }
                });
            }
        });
    }

    /**
     * 关闭KafkaAdminClient客户端
     */
    @After
    public void destroy() {
        kafkaAdminClient.close();
    }

}
