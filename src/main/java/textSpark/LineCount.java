//package textSpark;
//
//import org.apache.spark.SparkConf;
//import org.apache.spark.api.java.JavaPairRDD;
//import org.apache.spark.api.java.JavaRDD;
//import org.apache.spark.api.java.JavaSparkContext;
//import org.apache.spark.api.java.function.Function2;
//import org.apache.spark.api.java.function.PairFunction;
//import org.apache.spark.api.java.function.VoidFunction;
//import scala.Tuple2;
//
///**
// * Created by Administrator on 2018/8/30.
// */
//public class LineCount{
//
//
//    public static void main(String[] args) {
//        SparkConf conf = new SparkConf().setMaster("local").setAppName("ll");
//
//        JavaSparkContext sc = new JavaSparkContext(conf);
//
//        JavaRDD<String> line = sc.textFile("C:\\Users\\Administrator\\Desktop\\国和\\临时文件\\aaa.txt");
//
//        JavaPairRDD<String, Integer> lineAddOne = line.mapToPair(new PairFunction<String, String, Integer>() {
//            @Override
//            public Tuple2<String, Integer> call(String s) throws Exception {
//                return new Tuple2<String, Integer>(s, 1);
//            }
//        });
//
//        JavaPairRDD<String, Integer> lineCount = lineAddOne.reduceByKey(new Function2<Integer, Integer, Integer>() {
//            @Override
//            public Integer call(Integer v1, Integer v2) throws Exception {
//                return v1 + v2;
//            }
//        });
//        lineCount.foreach(new VoidFunction<Tuple2<String, Integer>>(){
//            @Override
//            public void call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
//                System.out.println(stringIntegerTuple2._1+"============"+stringIntegerTuple2._2);
//            }
//        });
//
//    }
//
//}
