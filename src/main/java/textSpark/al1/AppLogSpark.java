package textSpark.al1;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.codehaus.janino.Access;
import scala.Tuple2;

import java.util.List;

/**
 * Created by Administrator on 2018/9/21.
 */
public class AppLogSpark {
    public static void main(String[] args) throws Exception{
        SparkConf conf = new SparkConf().setAppName("ll").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        //JavaRDD<String> accesslogRDD = sc.textFile("C:\\Users\\Administrator\\Desktop\\国和\\临时文件\\linux\\access.log");
        JavaRDD<String> accesslogRDD = sc.textFile("hdfs://lkq:8020//access.log");
        JavaPairRDD<String, AccessLogInfo> stringAccessLogInfoJavaPairRDD = mapRDD2Pair(accesslogRDD);
        JavaPairRDD<String, AccessLogInfo> flhz = aggregateByDeviceID(stringAccessLogInfoJavaPairRDD);
        JavaPairRDD<AccessLogSort, String> accessSoty = accessSort(flhz);
        JavaPairRDD<AccessLogSort, String> accessSotySort = accessSoty.sortByKey(false);
        List<Tuple2<AccessLogSort, String>> take = accessSotySort.take(10);
        for(Tuple2<AccessLogSort, String> tt : take){
            System.out.println("============================"+tt+"==========================");
        }
        sc.close();
    }

    public static JavaPairRDD<String,AccessLogInfo> mapRDD2Pair(JavaRDD<String> accesslogRDD){
        return accesslogRDD.mapToPair(new PairFunction<String, String, AccessLogInfo>() {
            @Override
            public Tuple2<String, AccessLogInfo> call(String s) throws Exception {
                String[] split = s.split("\t");
                System.out.println("===========1111111================="+s+"==========================");
                return new Tuple2<String, AccessLogInfo>(split[1], new AccessLogInfo(Long.valueOf(split[0]), Long.valueOf(split[2]), Long.valueOf(split[3])));
            }
        });
    }

    public static   JavaPairRDD<String, AccessLogInfo> aggregateByDeviceID(JavaPairRDD<String, AccessLogInfo> stringAccessLogInfoJavaPairRDD){
        return stringAccessLogInfoJavaPairRDD.reduceByKey(new Function2<AccessLogInfo, AccessLogInfo, AccessLogInfo>() {
            @Override
            public AccessLogInfo call(AccessLogInfo v1, AccessLogInfo v2) throws Exception {
                long timestamp = v1.getTimestamp() > v2.getTimestamp() ? v2.getTimestamp(): v1.getTimestamp();
                long upTraffic = v1.getUpTraffic()+v2.getUpTraffic();
                long downTraffic = v1.getDownTraffic()+v2.getDownTraffic();
                return new AccessLogInfo(timestamp,upTraffic,downTraffic);
            }
        });
    }

    public static JavaPairRDD<AccessLogSort, String> accessSort( JavaPairRDD<String, AccessLogInfo> flhz){
        return flhz.mapToPair(new PairFunction<Tuple2<String, AccessLogInfo>, AccessLogSort, String>() {
            @Override
            public Tuple2<AccessLogSort, String> call(Tuple2<String, AccessLogInfo> stringAccessLogInfoTuple2) throws Exception {
                String key = stringAccessLogInfoTuple2._1;
                AccessLogInfo accessLogInfo = stringAccessLogInfoTuple2._2;
                long upTraffic = accessLogInfo.getUpTraffic();
                long downTraffic = accessLogInfo.getDownTraffic();
                long timestamp = accessLogInfo.getTimestamp();
                return new Tuple2<AccessLogSort, String>(new AccessLogSort(upTraffic,downTraffic,timestamp),key);
            }
        });
    }
}
