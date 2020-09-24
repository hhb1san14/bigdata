package com.hhb.zookeeper.monitor;

import org.I0Itec.zkclient.ZkClient;

/**
 * @description:
 * @author: huanghongbo
 * @date: 2020-07-26 19:48
 **/
public class ServerMain2 {

    private ZkClient zkClient = null;

    //初始化，判断是否有Servers目录
    public ServerMain2() {
        this.zkClient = new ZkClient("linux121:2181,linux122:2181,linux123:2181");
        if (!zkClient.exists("/servers")) {
            zkClient.createPersistent("/servers");
        }
    }


    //创建一个临时目录，告诉zookeeper，我这个服务上线了
    public void publish(String host, int port) {
        String path = zkClient.createEphemeralSequential("/servers/", host + ":" + port);
        System.err.println("创建成功，路径为：" + path + ", 值为：" + host + ":" + port);
    }


    /**
     * 主方法
     *
     * @param args
     */
    public static void main(String[] args) {
        ServerMain2 serverMain = new ServerMain2();
        //将自己的节点发不到zookeeper
        serverMain.publish(args[0], Integer.parseInt(args[1]));
        //创建时间服务
        new Thread(new TimeThreadServer(Integer.parseInt(args[1]))).start();
    }


}
