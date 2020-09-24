package com.hhb.redis;

import org.junit.Test;
import redis.clients.jedis.Jedis;

/**
 * @description:
 * @author: huanghongbo
 * @date: 2020-08-03 20:12
 **/
public class TestRedis {


    @Test
    public void testCoon() {
        Jedis jedis = new Jedis("59.110.241.53", 6379);
        jedis.set("key1", "value1");
        //获得Redis中字符串的值
        System.out.println(jedis.get("key1"));
        //在Redis中写list
        jedis.lpush("jedis:list:1", "1", "2", "3", "4", "5");
        //获得list的长度
        System.out.println(jedis.llen("jedis:list:1"));
    }

}
