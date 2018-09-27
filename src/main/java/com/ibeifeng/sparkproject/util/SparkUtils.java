package com.ibeifeng.sparkproject.util;

import com.alibaba.fastjson.JSONObject;
import com.ibeifeng.sparkproject.conf.ConfigurationManager;
import com.ibeifeng.sparkproject.constant.Constants;
import com.ibeifeng.sparkproject.test.MockData;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;

/**
 * Spark工具类
 */
public class SparkUtils {

    /**
     * TODO 根据当前是否本地测试的配置,
     * 决定如何设置SparkConf的master
     * @param conf
     */
    public static void setMaster(SparkConf conf){
        Boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
        if (local){
            conf.setMaster("local");
        }
    }

    /**
     * TODO 生成模拟数据,(只有本地模式生成模拟数据)
     * 如果spark.local配置为true生成模拟数据
     * @param sc
     * @param sqlContext
     */
    public static void mockData(JavaSparkContext sc, SQLContext sqlContext){
        boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
        if (local){
            MockData.mock(sc,sqlContext);
        }
    }

    /**
     * TODO 获取SQLContext
     * TODO 本地测试生成sqlcontext,生产环境生成HIVEContext对象
     * @param sc
     * @return
     */
    public static SQLContext getSQLContext (SparkContext sc){
        boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
        if (local){
            return new SQLContext(sc);
        }else {
            return new HiveContext(sc);
        }
    }

    /**
     * TODO 获取指定日期范围内的用户访问行为数据
     * @param sqlcontext
     * @param taskParam 任务参数
     * @return 行为数据RDD
     */
    public static JavaRDD<Row> getActionRDDByDateRange(SQLContext sqlcontext , JSONObject taskParam){
        String startDate = ParamUtils.getParam(taskParam,Constants.PARAM_START_DATE);
        String endDate = ParamUtils.getParam(taskParam,Constants.PARAM_END_DATE);
        String sql ="select * from user_visit_action where date>= '"+startDate+"' and date <= '"+endDate+"'";
        DataFrame actionDF = sqlcontext.sql(sql);
        /**
         * 这里就很有可能发生partiton数量过少的问题
         * 比如,sqparkSQL默认给第一个stage设置了20个task,但是根据你的数据量以及算法的复杂度,
         * 实际上需要1000个task
         *
         * 所以,这里,就可以对Spark SQL查出的RDD执行repartition重分区操作
         */
        //return actionDF.javaRDD().repartition(1000);
        return actionDF.javaRDD();
    }
}
