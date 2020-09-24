package com.hhb.zookeeper;

import org.I0Itec.zkclient.ZkClient;

/**
 * @description: 链接ZK集群创建节点
 * @author: huanghongbo
 * @date: 2020-07-24 13:56
 **/
public class ZkCreateNode {

    public static void main(String[] args) {
        ZkClient zkClient = new ZkClient("linux122:2181");
        //副节点不存在，直接创建自节点，属于及联创建，createPersistent方法默认不及联创建
        zkClient.createPersistent("/lagouClient/lagou-c1", true);
        System.err.println("创建成功");
    }

}
