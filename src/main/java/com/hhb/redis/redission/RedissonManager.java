package com.hhb.redis.redission;

import org.redisson.Redisson;
import org.redisson.config.Config;

/**
 * @description:
 * @author: huanghongbo
 * @date: 2020-08-09 14:34
 **/
public class RedissonManager {

    private static Config config = new Config(); //声明redisso对象
    private static Redisson redisson = null;

    //实例化redisson static{
    static {
        config.useClusterServers()
                // 集群状态扫描间隔时间，单位是毫秒
                .setScanInterval(2000)
                //cluster方式至少6个节点(3主3从，3主做sharding，3从用来保证主宕机后可以高可用)
                .addNodeAddress("redis://59.110.241.53:7001")
                .addNodeAddress("redis://59.110.241.53:7002")
                .addNodeAddress("redis://59.110.241.53:7003")
                .addNodeAddress("redis://59.110.241.53:7004")
                .addNodeAddress("redis://59.110.241.53:7005")
                .addNodeAddress("redis://59.110.241.53:7006");
        //得到redisson对象
        redisson = (Redisson) Redisson.create(config);
    }

    //获取redisson对象的方法
    public static Redisson getRedisson() {
        return redisson;
    }
}
