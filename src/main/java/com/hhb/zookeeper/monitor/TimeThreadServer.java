package com.hhb.zookeeper.monitor;

import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Date;

/**
 * @description:
 * @author: huanghongbo
 * @date: 2020-07-26 19:58
 **/
public class TimeThreadServer implements Runnable {


    private int port;

    public TimeThreadServer(int port) {
        this.port = port;
    }

    @Override
    public void run() {

        try {
            //要监听的端口号
            ServerSocket serverSocket = new ServerSocket(port);
            Socket accept = null;
            while (true) {
                accept = serverSocket.accept();
                OutputStream outputStream = accept.getOutputStream();
                outputStream.write(new Date().toLocaleString().getBytes());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
