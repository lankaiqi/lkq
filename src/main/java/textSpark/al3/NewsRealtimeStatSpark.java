package textSpark.al3;

import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by Administrator on 2018/9/23.
 */
public class NewsRealtimeStatSpark {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("ll").setMaster("local");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));
        // 创建输入DStream
        Map<String, String> kafkaParams = new HashMap<String, String>();
        kafkaParams.put("metadata.broker.list", "lkq:9092,lkq2:9092,lkq3:9092");
        Set<String> topics = new HashSet<String>();
        topics.add("news-access");
        JavaPairInputDStream<String, String> lines = KafkaUtils.createDirectStream(
                jssc,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                kafkaParams,
                topics);
        // 过滤出访问日志
        JavaPairDStream<String, String> accessDStream = lines.filter(new Function<Tuple2<String, String>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, String> v1) throws Exception {
                String[] split = v1._2.split(" ");
                if("view".equals(split[5])) {
                    return true;
                } else {
                    return false;
                }
            }
        });

        // 统计第一个指标：每10秒内的各个页面的pv
       // calculatePagePv(accessDStream);
        // 统计第二个指标：每10秒内的各个页面的uv
        //calculatePageUv(accessDStream);
        // 统计第三个指标：实时注册用户数
        //zcuserid(lines);
        // 统计第四个指标：实时注册用户数
        ydtcs(accessDStream);

        jssc.start();
        jssc.awaitTermination();
        jssc.close();
    }

    public  static void calculatePagePv( JavaPairDStream<String, String> accessDStream){
        JavaPairDStream<Long, Long> longLongJavaPairDStream = accessDStream.mapToPair(new PairFunction<Tuple2<String, String>, Long, Long>() {
            @Override
            public Tuple2<Long, Long> call(Tuple2<String, String> tuple) throws Exception {
                String[] split = tuple._2.split(" ");
                Long pageid = Long.valueOf(split[3]);
                return new Tuple2<Long, Long>(pageid, 1L);
            }
        });
        JavaPairDStream<Long, Long> pagePvDStream = longLongJavaPairDStream.reduceByKey(new Function2<Long, Long, Long>() {
            @Override
            public Long call(Long v1, Long v2) throws Exception {
                return v1 + v2;
            }
        });
        pagePvDStream.print();
    }

    public  static void calculatePageUv( JavaPairDStream<String, String> accessDStream){
        JavaDStream<String> map = accessDStream.map(new Function<Tuple2<String, String>, String>() {
            @Override
            public String call(Tuple2<String, String> v1) throws Exception {
                String[] split = v1._2.split(" ");
                return split[3] + "_" + ("null".equalsIgnoreCase(split[2]) ? "-1" : split[2]);
            }
        });
        JavaDStream<String> distinctPageidUseridDStream = map.transform(new Function<JavaRDD<String>, JavaRDD<String>>() {
            @Override
            public JavaRDD<String> call(JavaRDD<String> v1) throws Exception {
                return v1.distinct();
            }
        });
        JavaPairDStream<Long, Long> longLongJavaPairDStream = distinctPageidUseridDStream.mapToPair(new PairFunction<String, Long, Long>() {
            @Override
            public Tuple2<Long, Long> call(String s) throws Exception {
                String[] split = s.split("_");
                return new Tuple2<Long, Long>(Long.valueOf(split[0]), 1L);
            }
        });
        JavaPairDStream<Long, Long> longLongJavaPairDStream1 = longLongJavaPairDStream.reduceByKey(new Function2<Long, Long, Long>() {
            @Override
            public Long call(Long v1, Long v2) throws Exception {
                return v1 + v2;
            }
        });
        longLongJavaPairDStream1.print();
    }
    public  static void  zcuserid(JavaPairInputDStream<String, String> lines){
        JavaPairDStream<String, String> filter = lines.filter(new Function<Tuple2<String, String>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, String> v1) throws Exception {
                String s = v1._2.split(" ")[5];
                if ("register".equals(s)) {
                    System.out.println("============================"+true+"==========================");
                    return true;
                } else {
                    System.out.println("============================"+false+"==========================");
                    return false;
                }
            }
        });
        JavaDStream<Long> count = filter.count();
        count.print();
    }

    public  static void  ydtcs(JavaPairDStream<String, String> accessDStream){
        JavaPairDStream<String, Long> stringLongJavaPairDStream = accessDStream.mapToPair(new PairFunction<Tuple2<String, String>, String, Long>() {
            @Override
            public Tuple2<String, Long> call(Tuple2<String, String> stringStringTuple2) throws Exception {
                String[] split = stringStringTuple2._2.split(" ");
                return new Tuple2<String, Long>(("null".equalsIgnoreCase(split[2]) ? "-1" : split[2]),1L);
            }
        });
        JavaPairDStream<String, Long> stringLongJavaPairDStream1 = stringLongJavaPairDStream.reduceByKey(new Function2<Long, Long, Long>() {
            @Override
            public Long call(Long v1, Long v2) throws Exception {
                return v1 + v2;
            }
        });
        stringLongJavaPairDStream1.count().print();
        JavaPairDStream<String, Long> filter = stringLongJavaPairDStream1.filter(new Function<Tuple2<String, Long>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, Long> v1) throws Exception {
                if (v1._2 == 1L) {
                    return true;
                } else {
                    return false;
                }
            }
        });
        JavaDStream<Long> count = filter.count();
        count.print();
    }
}
