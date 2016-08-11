package com.mycom.bds.common.util;

public class A {

    public static void main(String[] args) {
        System.out.println("A");
        System.out.println("B");
        try {
            Thread.currentThread().sleep(Long.MAX_VALUE);
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

}
