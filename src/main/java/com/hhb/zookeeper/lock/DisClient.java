package com.hhb.zookeeper.lock;

import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.lang.StringUtils;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * @description: 抢锁
 * 1. 去zk创建临时序列节点，并获取到序号
 * 2. 判断⾃己创建节点序号是否是当前节点最⼩序号，如果是则获取锁 执行相关操作，最后要释放锁
 * 3. 不是最⼩节点，当前线程需要等待，等待你的前一个序号的节点被删除，然后再次判断⾃己是否是最⼩节点。。。
 * @author: huanghongbo
 * @date: 2020-07-26 20:22
 **/
public class DisClient {


    private ZkClient zkClient;

    String currNode = null;

    String preNode = null;

    private String lockPath = "/lock/";

    private String childrenPath = "/lock";

    private CountDownLatch countDownLatch = new CountDownLatch(1);

    public DisClient() {
        this.zkClient = new ZkClient("linux121:2181,linux122:2181,linux123:2181");
    }


    public void getLock() {
        String name = Thread.currentThread().getName();
        if (tryGetLock()) {
            System.err.println("线程：" + name + "获取到了锁");
        } else {
            System.err.println(name + ":获取锁失败,进⼊入等待状态");
            //等待获取锁
            waitLock();
            //再次尝试获取锁
            getLock();
        }
    }

    //判断是否获取到了锁
    private boolean tryGetLock() {
        if (StringUtils.isEmpty(currNode)) {
            //在lock目录下面创建临时顺序文件夹
            currNode = zkClient.createEphemeralSequential(lockPath, 1);
            System.err.println("currNode=>" + currNode);
        }
        List<String> childrenPath = zkClient.getChildren(this.childrenPath);
        System.err.println("childrenPath===>>>" + childrenPath);
        //默认升序，则下标索引为0的，为最小目录
        Collections.sort(childrenPath);
        //如果最小的节点目录就是自己，说明自己获取锁
        if (currNode.endsWith(childrenPath.get(0))) {
            return true;
        }
        //否则查找当前节点的前一个节点
        int i = Collections.binarySearch(childrenPath, currNode.substring(lockPath.length()));
        preNode = lockPath + childrenPath.get(i - 1);
        return false;
    }


    //订阅比自己小的节点，等待获取锁
    private void waitLock() {

        //监听器
        IZkDataListener iZkDataListener = new IZkDataListener() {
            @Override
            public void handleDataChange(String s, Object o) throws Exception {

            }

            @Override
            public void handleDataDeleted(String s) throws Exception {
                countDownLatch.countDown();
            }
        };
        System.err.println("当前节点为：" + currNode + "监控的节点为:" + preNode);
        zkClient.subscribeDataChanges(preNode, iZkDataListener);

        if (zkClient.exists(preNode)) {
            try {
                countDownLatch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        //解除监听
        zkClient.unsubscribeDataChanges(preNode, iZkDataListener);
    }

    public void unLock() {
        zkClient.delete(currNode);
        zkClient.close();
    }

}
