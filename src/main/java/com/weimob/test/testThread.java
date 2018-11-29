package com.weimob.test;

/**
 * Created by dell on 2018/11/8.
 */
public class testThread {
    public static void main(String[] agrs) {
        Thread tt1 = new Thread(new TT1());
        Thread tt2 = new Thread(new TT2());
//        tt1.setPriority(Thread.NORM_PRIORITY + 3); //T1的普通优先级往上+3
        tt1.start();
        tt2.start();
    }
}

class TT1 implements Runnable {
    public void run() {
        for(int i=0 ; i < 1000 ; i++) {
            System.out.println("T1" + i);
        }
    }
}

class TT2 implements Runnable {
    public void run() {
        for(int i=0 ; i < 1000 ; i++) {
            System.out.println("-------T2" + i);
        }
    }
}
