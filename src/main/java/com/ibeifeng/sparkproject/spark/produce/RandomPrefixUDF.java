package com.ibeifeng.sparkproject.spark.produce;

import org.apache.spark.sql.api.java.UDF2;

import java.util.Random;

/**
 * random_prefix()
 */
public class RandomPrefixUDF implements UDF2<String,Integer,String>{
    @Override
    public String call(String val, Integer num) throws Exception {
        Random random  = new Random();
        int ranNum = random.nextInt(10);
        return ranNum+"_"+val;
    }
}
