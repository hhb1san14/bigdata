package com.hhb.zookeeper;

import org.I0Itec.zkclient.ZkClient;

/**
 * @description: 删除节点
 * @author: huanghongbo
 * @date: 2020-07-24 14:04
 **/
public class ZkDeleteNode {

    public static void main(String[] args) {

        ZkClient zkClient = new ZkClient("linux122:2181");

        //delete删除的方式，只能删除目录下没有子节点的目录
//        zkClient.delete("/hhb-test");

        //及联递归的删除，把要删除的目录下面的子节点全都删除。
        //是先删除子节点，在删除父节点
        zkClient.deleteRecursive("/lagouClient");

    }
}
