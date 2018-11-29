package com.weimob.test;

import java.util.Random;

/**
 * Created by dell on 2018/11/12.
 */
public class TestRandom {
    public static void main(String[] args) {
        Random random = new Random();
        for (int i = 0 ; i < 20 ; i++) {
            int ran1 = random.nextInt(10000);
            System.out.println("aaaaa = " + ran1);
        }
    }
}
