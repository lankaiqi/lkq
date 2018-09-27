package com.ibeifeng.sparkproject.spark.produce;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.spark.sql.api.java.UDF1;

/**
 * 去除随机前缀
 * remove_random_prefix()
 */
public class RemoveRandomPrefixUDF implements UDF1<String,String> {
    @Override
    public String call(String val) throws Exception {
        String[] valSplited = val.split("_");
        return valSplited[1];
    }
}
