package com.ibeifeng.sparkproject;

import com.ibeifeng.sparkproject.conf.ConfigurationManager;

/**
 * Created by Administrator on 2018/7/15.
 */
public class ConfigurationManagerTest {
    public static void main (String[] args){
        String test1 = ConfigurationManager.getProperty("test1");
        String test2 = ConfigurationManager.getProperty("test2");
        System.out.println("============================"+test1+"==========================");
        System.out.println("============================"+test2+"==========================");
    }
}
