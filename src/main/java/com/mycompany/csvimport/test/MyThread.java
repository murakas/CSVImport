package com.mycompany.csvimport.test;

public class MyThread extends Thread {

    int position = 0;
    int limit;
    int mils;

    public MyThread(int limit, int mils) {
        this.limit = limit;
        this.mils = mils;
    }

    public void run() {
        while (position < limit) {
            try {
                Thread.sleep(mils);
            } catch (InterruptedException e) {
            }
            position++;
        }
    }

    public int getPosition() {
        return position;
    }
}
