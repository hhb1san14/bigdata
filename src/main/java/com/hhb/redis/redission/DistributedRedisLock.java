package com.hhb.redis.redission;

import org.redisson.Redisson;
import org.redisson.api.RLock;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * @description:
 * @author: huanghongbo
 * @date: 2020-08-09 14:37
 **/
public class DistributedRedisLock {
    //从配置类中获取redisson对象
    private static Redisson redisson = RedissonManager.getRedisson();
    private static final String LOCK_TITLE = "redisLock_"; //加锁

    public static boolean acquire(String lockName) { //声明key对象
        String key = LOCK_TITLE + lockName; //获取锁对象
        RLock mylock = redisson.getLock(key); //加锁，并且设置锁过期时间3秒，防止死锁的产生 uuid+threadId
        mylock.lock(2, TimeUnit.SECONDS); //加锁成功
        return true;
    }

    //锁的释放
    public static void release(String lockName) {
        //必须是和加锁时的同一个key
        String key = LOCK_TITLE + lockName;
        //获取所对象
        RLock mylock = redisson.getLock(key);
        //释放锁(解锁)
        mylock.unlock();
    }


    public String discount() throws IOException {
        String key = "lock001";
        //加锁
        DistributedRedisLock.acquire(key); //执行具体业务逻辑
        //todo:dosoming
        //释放锁
        DistributedRedisLock.release(key);
        //返回结果 return soming;
        return null;
    }
}
