package com.hhb.zookeeper.lock;


import java.util.concurrent.TimeUnit;

/**
 * @description:
 * @author: huanghongbo
 * @date: 2020-07-26 20:41
 **/
public class TestMain {

    public static void main(String[] args) {
        for (int i = 0; i < 5; i++) {
            new Thread(new ThreadClient()).start();
        }
    }

    static class ThreadClient implements Runnable {

        private DisClient disClient = new DisClient();

        @Override
        public void run() {
            disClient.getLock();
            try {
                TimeUnit.SECONDS.sleep(2);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.err.println("==========");
            disClient.unLock();
        }
    }


}
