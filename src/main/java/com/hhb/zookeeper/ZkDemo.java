package com.hhb.zookeeper;

import org.I0Itec.zkclient.ZkClient;

/**
 * @program:
 * @description:
 * @author: huanghongbo
 * @date: 2020-07-24 13:37
 **/
public class ZkDemo {

    public static void main(String[] args) {
        // 获取ZkClient对象，客户端与集群通信的端口是2181
        //建立了到ZK集群的会话
        ZkClient zkClient = new ZkClient("linux122:2181");
        System.err.println("zkClient is ready");
    }

}
