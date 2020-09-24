package com.hhb.redis;

import redis.clients.jedis.Jedis;

import java.util.Collections;

/**
 * @description:
 * @author: huanghongbo
 * @date: 2020-08-09 14:11
 **/
public class TestLock {

    public static void main(String[] args) {

    }


    /**
     * 原子性操作
     *
     * @return
     */
    public static boolean getLock() {
        Jedis jedis = new Jedis("59.110.241.53", 6381);
        String set = jedis.set("k1", "v1", "NX", "PX", 1000);
        if ("OK".equals(set)) {
            return true;
        }
        return false;
    }

    /**
     * 非原子性操作，设置过期时间的时候，系统挂了，后面都获取不到锁了
     *
     * @return
     */
    public static boolean getLock2() {
        Jedis jedis = new Jedis("59.110.241.53", 6381);
        Long set = jedis.setnx("k1", "v1");
        if (1 == set) {
            jedis.expire("k1", 1000);
            return true;
        }
        return false;
    }

    /**
     * 释放分布式锁
     *
     * @param lockKey * @param requestId
     */
    public static void releaseLockUnSafe(String lockKey, String requestId) {
        Jedis jedis = new Jedis("59.110.241.53", 6381);
        if (requestId.equals(jedis.get(lockKey))) {
            jedis.del(lockKey);
        }
    }


    /**
     * 安全的
     *
     * @param lockKey
     * @param requestId
     * @return
     */
    public static boolean releaseLockSafe(String lockKey, String requestId) {
        Jedis jedis = new Jedis("59.110.241.53", 6381);
        String script = "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end ";
        Object result = jedis.eval(script, Collections.singletonList(lockKey),
                Collections.singletonList(requestId));
        if (result.equals(1L)) {
            return true;
        }
        return false;
    }
}