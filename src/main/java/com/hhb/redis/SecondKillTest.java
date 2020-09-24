package com.hhb.redis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @description:
 * @author: huanghongbo
 * @date: 2020-08-09 13:47
 **/
public class SecondKillTest {


    public static void main(String[] args) {
        String redisKey = "redisKey";
        ExecutorService executorService = Executors.newFixedThreadPool(20);
        //设置初始值
        Jedis jedis = new Jedis("59.110.241.53", 6381);
        jedis.set(redisKey, "0");
        jedis.close();

        //并发竞争
        for (int i = 0; i < 1000; i++) {
            executorService.execute(() -> {
                Jedis j1 = new Jedis("59.110.241.53", 6381);
                j1.watch(redisKey);
                String redisValue = j1.get(redisKey);
                int valInteger = Integer.valueOf(redisValue);
                String userInfo = UUID.randomUUID().toString();
                if (valInteger < 20) {
                    Transaction multi = j1.multi();
                    multi.incr(redisKey);
                    List<Object> exec = multi.exec();
                    if (exec != null && exec.size() > 0) {
                        System.out.println("用户:" + userInfo + "，秒杀成功! 当前成功人数:" + (valInteger + 1));
                    } else {
                        System.out.println("用户:" + userInfo + "，秒杀失败");
                    }
                } else {
                    System.out.println("已经有20人秒杀成功，秒杀结束");
                }
                j1.close();
            });
        }
        executorService.shutdown();
    }


}
