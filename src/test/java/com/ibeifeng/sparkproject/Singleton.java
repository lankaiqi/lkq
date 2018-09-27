package com.ibeifeng.sparkproject;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by Administrator on 2018/7/15.
 */
public class Singleton {
    //首先一个私有的静态变量,来引用自己即将被创建出来的单例
    private static Singleton instance = null;

    /**
     * 其次必须私有构造方法
     */
    private Singleton(){}

    /**
     *共有的静态方法,创建唯一实例
     * @return
     */
    public static Singleton getInstance(){
        if (instance ==null){
            synchronized(Singleton.class) {
                if (instance == null) {
                    instance = new Singleton();
                }
            }
        }
        return instance;
    }
 /*   public static class Aaa{
        public static  LinkedList<Integer> ll = new LinkedList<Integer>();
        public static synchronized  void bbb (int a){
            ll.add(a);
        }
    }
       public static void main (String[] args){
        Thread thread = new Thread(){
            public void run(){
                for (int a=1; a<=10000;a++){
                    //Aaa.ll.add(a);
                    Aaa.bbb(a);
                }
                System.out.println("============================"+1+"==========================");
            }
        };
        Thread thread2 = new Thread(){
            public void run(){
                for (int a=1; a<=10000;a++){
                    Aaa.bbb(a);
                }
                System.out.println("============================"+2+"==========================");
            }
        };
        Thread thread3 = new Thread(){
            public void run(){
                for (int a=1; a<=10000;a++){
                    Aaa.bbb(a);
                }
                System.out.println("============================"+3+"==========================");
            }
        };
        Thread thread4 = new Thread(){
            public void run(){
                for (int a=1; a<=10000;a++){
                    Aaa.bbb(a);
                }
                System.out.println("============================"+4+"==========================");
            }
        };
        Thread thread5 = new Thread(){
            public void run(){
                for (int a=1; a<=10000;a++){
                    Aaa.bbb(a);
                }
                System.out.println("============================"+5+"==========================");
            }
        };
        Thread thread6 = new Thread(){
            public void run(){
                for (int a=1; a<=10000;a++){
                    Aaa.bbb(a);
                }
                System.out.println("============================"+6+"==========================");
            }
        };
        thread.start();
        thread2.start();
        thread3.start();
        thread4.start();
        thread5.start();
        thread6.start();
           try {
               Thread.sleep(1000);
           } catch (InterruptedException e) {
               e.printStackTrace();
           } finally {
               System.out.println("============================"+Aaa.ll.size()+"==========================");
           }
    }*/
}
