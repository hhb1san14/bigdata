package com.hhb.zookeeper.monitor;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.ZkClient;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * @description:
 * @author: huanghongbo
 * @date: 2020-07-26 20:04
 **/
public class ClientMain {


    private ZkClient zkClient;

    //获取路径
    private List<String> dataList = new ArrayList<>();

    public ClientMain() {
        this.zkClient = new ZkClient("linux121:2181,linux122:2181,linux123:2181");
    }


    public void subscription() {
        //获取初始节点
        List<String> list = zkClient.getChildren("/servers");
        for (int i = 0; i < list.size(); i++) {
            Object o = zkClient.readData("/servers/" + list.get(i));
            dataList.add(String.valueOf(o));
        }
        //监控/servers目录下面的节点
        zkClient.subscribeChildChanges("/servers", new IZkChildListener() {
            @Override
            public void handleChildChange(String s, List<String> list) throws Exception {
                List<String> dlist = new ArrayList<>();
                for (int i = 0; i < list.size(); i++) {
                    Object o = zkClient.readData("/servers/" + list.get(i));
                    dlist.add(String.valueOf(o));
                }
                dataList = dlist;
                System.err.println("接收到服务器的最新变化信息为：" + dataList);
            }
        });
    }

    public void getTime() {
        String ipPort = dataList.get(new Random().nextInt(dataList.size()));
        String[] ipPortArr = ipPort.split(":");
        Socket socket = null;
        InputStream inputStream = null;
        OutputStream outputStream = null;
        try {
            socket = new Socket(ipPortArr[0], Integer.parseInt(ipPortArr[1]));
            inputStream = socket.getInputStream();
            outputStream = socket.getOutputStream();
            //请求数据
            outputStream.write("query time ....".getBytes());
            outputStream.flush();

            byte[] bytes = new byte[1024];
            inputStream.read(bytes);
            System.out.println("client端接收到server:+" + ipPort + "+返回结果:" + new String(bytes));
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                outputStream.close();
                inputStream.close();
                socket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


    public static void main(String[] args) throws InterruptedException {
        ClientMain clientMain = new ClientMain();
        clientMain.subscription();
        while (true) {
            clientMain.getTime();
            TimeUnit.SECONDS.sleep(2);
        }
    }


}