package textSpark;

import com.google.common.base.*;
import com.google.common.base.Optional;
import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.*;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.sql.Array;
import java.sql.Connection;
import java.sql.Statement;
import java.util.*;

/**
 * Created by Administrator on 2018/9/17.
 */
public class StreamingWordCount {
    public static void main(String[] args) {
       // first();
        //hdfs();
        //kafkaReceiver();
       // kafkaDirect();
        //updateStateByKey();
       // blackGL();
       // hdck();
        top3();
    }

    public static void first(){
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("ll");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));
        JavaReceiverInputDStream<String> lines = jssc.socketTextStream("lkq", 9999);
        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" "));
            }
        });
        JavaPairDStream<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        });
        JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        wordCounts.print();


        jssc.start();
        jssc.awaitTermination();
        jssc.close();
    }


    public static void hdfs(){
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("ll");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));
       // JavaReceiverInputDStream<String> lines = jssc.socketTextStream("lkq", 9999);
        JavaDStream<String> lines = jssc.textFileStream("C:\\Users\\Administrator\\Desktop\\国和\\临时文件\\hdfs\\aaa.txt");

        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" "));
            }
        });
        JavaPairDStream<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        });
        JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        wordCounts.print();


        jssc.start();
        jssc.awaitTermination();
        jssc.close();
    }


    public static void kafkaReceiver(){
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("ll");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));
        HashMap<String, Integer> topicMap = new HashMap<String, Integer>();
        topicMap.put("spark",1);
        JavaPairReceiverInputDStream<String, String> lines = KafkaUtils.createStream(jssc, "lkq:2181,lkq2:2181,lkq3:2181", "dfd", topicMap);
        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<Tuple2<String, String>, String>() {
            @Override
            public Iterable<String> call(Tuple2<String, String> vv) throws Exception {
                System.out.println("============================"+vv._1+"==========================");
                return Arrays.asList(vv._2.split(" "));
            }
        });

        JavaPairDStream<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        });
        JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        wordCounts.print();

        jssc.start();
        jssc.awaitTermination();
        jssc.close();
    }

    public static void kafkaDirect(){
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("ll");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));
        HashMap<String, String> kafkaParams = new HashMap<String, String>();
        kafkaParams.put("metadata.broker.list","lkq:9092,lkq2:9092,lkq3:9092");
        Set<String> topics = new HashSet<String>();
        topics.add("spark");
        JavaPairInputDStream<String, String> lines = KafkaUtils.createDirectStream(jssc, String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topics);
        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<Tuple2<String, String>, String>() {
            @Override
            public Iterable<String> call(Tuple2<String, String> vv) throws Exception {
                return Arrays.asList(vv._2.split(" "));
            }
        });

        JavaPairDStream<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        });
        JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        wordCounts.print();

        jssc.start();
        jssc.awaitTermination();
        jssc.close();
    }

    public static void updateStateByKey(){
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("ll");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));
        jssc.checkpoint("C:\\Users\\Administrator\\Desktop\\国和\\临时文件\\checkpoint");
        HashMap<String, String> kafkaParams = new HashMap<String, String>();
        kafkaParams.put("metadata.broker.list","lkq:9092,lkq2:9092,lkq3:9092");
        Set<String> topics = new HashSet<String>();
        topics.add("spark");
        JavaPairInputDStream<String, String> lines = KafkaUtils.createDirectStream(jssc, String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topics);
        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<Tuple2<String, String>, String>() {
            @Override
            public Iterable<String> call(Tuple2<String, String> vv) throws Exception {
                return Arrays.asList(vv._2.split(" "));
            }
        });

        JavaPairDStream<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        });
        JavaPairDStream<String, Integer> counts = pairs.updateStateByKey(new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {

            @Override
            public Optional<Integer> call(List<Integer> values, Optional<Integer> states) throws Exception {
                Integer newValues = 0;
                if (states.isPresent()) {
                    newValues = states.get();
                }
                for (Integer ii : values) {
                    newValues += ii;
                }
                return Optional.of(newValues);
            }
        });
        counts.print();

        jssc.start();
        jssc.awaitTermination();
        jssc.close();
    }


    public static void blackGL(){
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("ll");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));
        ArrayList<Tuple2<String, Boolean>> blacklistData = new ArrayList<Tuple2<String, Boolean>>();
        blacklistData.add(new Tuple2<String, Boolean>("tom",true));
        final JavaPairRDD<String, Boolean> stringBooleanJavaPairRDD = jssc.sc().parallelizePairs(blacklistData);

        JavaReceiverInputDStream<String> adsClickLog = jssc.socketTextStream("lkq", 9999);
        JavaPairDStream<String, String> stringStringJavaPairDStream = adsClickLog.mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String s) throws Exception {
                return new Tuple2<String, String>(s.split(" ")[1], s);
            }
        });

        JavaDStream<String> transform = stringStringJavaPairDStream.transform(new Function<JavaPairRDD<String, String>, JavaRDD<String>>() {
            @Override
            public JavaRDD<String> call(JavaPairRDD<String, String> v1) throws Exception {
                JavaPairRDD<String, Tuple2<String, Optional<Boolean>>> stringTuple2JavaPairRDD = v1.leftOuterJoin(stringBooleanJavaPairRDD);
                JavaPairRDD<String, Tuple2<String, Optional<Boolean>>> filter =  stringTuple2JavaPairRDD.filter(new Function<Tuple2<String, Tuple2<String, Optional<Boolean>>>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<String, Tuple2<String, Optional<Boolean>>> v1) throws Exception {
                        if (v1._2._2.isPresent() && v1._2._2.get()) {
                            return false;
                        }
                        return true;
                    }
                });
                JavaRDD<String> log = filter.map(new Function<Tuple2<String, Tuple2<String, Optional<Boolean>>>, String>() {
                    @Override
                    public String call(Tuple2<String, Tuple2<String, Optional<Boolean>>> v1) throws Exception {
                        return v1._2._1;
                    }
                });
                return log;
            }
        });

        transform.print();


        jssc.start();
        jssc.awaitTermination();
        jssc.close();
    }


    public static void hdck(){
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("ll");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));
        JavaReceiverInputDStream<String> lines = jssc.socketTextStream("lkq", 9999);
        JavaPairDStream<String, Integer> qiefen = lines.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s,1);
            }
        });
        JavaPairDStream<String, Integer> stringIntegerJavaPairDStream = qiefen.reduceByKeyAndWindow(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1+v2;
            }
        },Durations.seconds(60),Durations.seconds(10));
        JavaPairDStream<String, Integer> stringIntegerJavaPairDStream1 = stringIntegerJavaPairDStream.transformToPair(
                new Function<JavaPairRDD<String, Integer>, JavaPairRDD<String, Integer>>() {
            @Override
            public JavaPairRDD<String, Integer> call(JavaPairRDD<String, Integer> tuple) throws Exception {

                JavaPairRDD<Integer, String> pairRDD = tuple.mapToPair(new PairFunction<Tuple2<String,Integer>, Integer, String>() {
                    @Override
                    public Tuple2<Integer, String> call(Tuple2<String, Integer> vvv) throws Exception {
                        return new Tuple2<Integer, String>(vvv._2,vvv._1);
                    }
                });
                JavaPairRDD<Integer, String>  sortedCount  = pairRDD.sortByKey(false);

                JavaPairRDD<String, Integer> pairRDDHx = sortedCount.mapToPair(new PairFunction<Tuple2<Integer,String>, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(Tuple2<Integer, String> tu) throws Exception {
                        return new Tuple2<String, Integer>(tu._2,tu._1);
                    }
                });
                List<Tuple2<String, Integer>> take = pairRDDHx.take(3);
                for(Tuple2<String, Integer> tt :take){
                    System.out.println(tt._1+"-------------------"+tt._2);
                }
                return pairRDDHx;
            }
        });
       // stringIntegerJavaPairDStream1.print();

        stringIntegerJavaPairDStream1.foreachRDD(new Function<JavaPairRDD<String, Integer>, Void>() {
            @Override
            public Void call(JavaPairRDD<String, Integer> v1) throws Exception {
                v1.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Integer>>>() {
                    @Override
                    public void call(Iterator<Tuple2<String, Integer>> tuple2Iterator) throws Exception {
                        Connection conn = ConnectionPll.getConnection();
                        Tuple2<String,Integer> typle =null;
                        while(tuple2Iterator.hasNext()){
                            typle=tuple2Iterator.next();
                            String sql ="insert into aa(a1,a2) values ('"+typle._1+"','"+typle._2+"')";
                            Statement statement = conn.createStatement();
                            statement.executeUpdate(sql);
                        }
                    }
                });
                return null;
            }
        });
        jssc.start();
        jssc.awaitTermination();
        jssc.close();
    }


    public static void top3(){
        SparkConf conf = new SparkConf().setAppName("ll");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));
        JavaReceiverInputDStream<String> lines = jssc.socketTextStream("lkq", 9999);
        JavaPairDStream<String, Integer> tuple2 = lines.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        });
        JavaPairDStream<String, Integer> reducebyKeyWindow = tuple2.reduceByKeyAndWindow(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1+v2;
            }
        },Durations.seconds(60),Durations.seconds(10));

        reducebyKeyWindow.foreachRDD(new Function<JavaPairRDD<String, Integer>, Void>() {
            @Override
            public Void call(JavaPairRDD<String, Integer> v1) throws Exception {
                JavaRDD<Row> categoryRDD = v1.map(new Function<Tuple2<String,Integer>, Row>() {
                    @Override
                    public Row call(Tuple2<String, Integer> v1) throws Exception {
                        String category = v1._1.split(" ")[0];
                        String product = v1._1.split(" ")[1];
                        Integer count = v1._2;
                        return RowFactory.create(category,product,count);
                    }
                });
                ArrayList<StructField> structFields = new ArrayList<StructField>();
                structFields.add(DataTypes.createStructField("category", DataTypes.StringType,true));
                structFields.add(DataTypes.createStructField("product", DataTypes.StringType,true));
                structFields.add(DataTypes.createStructField("click_count", DataTypes.IntegerType,true));
                StructType structType = DataTypes.createStructType(structFields);
                HiveContext sqlContext = new HiveContext(categoryRDD.context());
                DataFrame dataFrame = sqlContext.createDataFrame(categoryRDD, structType);
                dataFrame.registerTempTable("product_click_log");
                DataFrame sql = sqlContext.sql("select category,product,click_count " +
                        "from ( " +
                        "select " +
                        "category,product,click_count, " +
                        "row_number() OVER (PARTITION BY category ORDER BY click_count DESC ) rank " +
                        "FROM product_click_log " +
                        ") tmp " +
                        "where  rank<=3");
                sql.show();
                return null ;
            }
        });


        jssc.start();
        jssc.awaitTermination();
        jssc.close();
    }
}
