package com.weimob.test;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.lang.reflect.Method;

/**
 * Created by dell on 2018/11/7.
 */
public class TestFZ {
    public static void main(String[] args) throws InterruptedException {
        OperatingSystemMXBean operatingSystemMXBean = ManagementFactory.getOperatingSystemMXBean();


        while (true) {
            double load;
            int cpu = operatingSystemMXBean.getAvailableProcessors();
            try {
                Method method = OperatingSystemMXBean.class.getMethod("getSystemLoadAverage");
                load = (Double) method.invoke(operatingSystemMXBean);
                System.out.println("WARN!!load:" + load + ","+ "cpu:" + cpu);
            } catch (Throwable e) {
                load = -1;
            }
            cpu = operatingSystemMXBean.getAvailableProcessors();
            if (load >= cpu) {
                System.err.println("WARN!!load:" + load + ","+ "cpu:" + cpu);
            }
            Thread.currentThread().sleep(1 * 1000);
        }
    }
}
