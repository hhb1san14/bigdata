package com.hhb.redis;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPoolConfig;

import java.util.HashSet;
import java.util.Set;

/**
 * @description:
 * @author: huanghongbo
 * @date: 2020-08-08 20:34
 **/
public class TestRedisCluster {


    //测试Redis集群
    public static void main(String[] args) {
        JedisPoolConfig config = new JedisPoolConfig();
        Set<HostAndPort> jedisClusterNode = new HashSet<>();
        jedisClusterNode.add(new HostAndPort("59.110.241.53", 7001));
        jedisClusterNode.add(new HostAndPort("59.110.241.53", 7002));
        jedisClusterNode.add(new HostAndPort("59.110.241.53", 7003));
        jedisClusterNode.add(new HostAndPort("59.110.241.53", 7004));
        jedisClusterNode.add(new HostAndPort("59.110.241.53", 7005));
        jedisClusterNode.add(new HostAndPort("59.110.241.53", 7006));
        jedisClusterNode.add(new HostAndPort("59.110.241.53", 7007));
        JedisCluster jcd = new JedisCluster(jedisClusterNode, config);
        jcd.set("k1", "v1");
        String value = jcd.get("k1");
        System.err.println(value);
    }
}
