package com.mycompany.csvimport.test;

import java.util.HashSet;
import java.util.Set;

public class Test {
    public static void main(String[] args) {
        MyThread thread1 = new MyThread(20, 500);
        MyThread thread2 = new MyThread(30, 200);
        Set<MyThread> myThreads = new HashSet<>();
        myThreads.add(thread1);
        myThreads.add(thread2);
        thread1.start();
        thread2.start();

        while (thread1.isAlive() || thread2.isAlive()) {
            for (MyThread mt : myThreads) {
                System.out.println(mt.getId() + ": " + mt.getPosition());
            }
        }
    }
}
