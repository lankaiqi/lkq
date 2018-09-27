package textSpark;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.*;

/**
 * Created by Administrator on 2018/8/30.
 */
public class Map2{


    public static void main(String[] args) {
        //map();
        //filter();
        //flatMap();
        //groupbykey();
       // reducebykey();
       // sortbykey();
       // join();
       // reduce();
      //  collect();
      //  count();
       //countbykey();
       // broadcast();
       // accumulatorVariable();
       // px();
        //zdypx();
        fztop3();
    }

    public static void map(){
        SparkConf conf = new SparkConf().setMaster("local").setAppName("ll");

        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> list = Arrays.asList(1,2,3,4,5);

        JavaRDD<Integer> parallelize = sc.parallelize(list);

        JavaRDD<Integer> map = parallelize.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer v1) throws Exception {
                return v1 * 2;
            }
        });
        map.foreach(new VoidFunction<Integer>(){
            @Override
            public void call(Integer o) throws Exception {
                System.out.println("============================"+o+"==========================");
            }
        });
    }



    public static void filter(){
        SparkConf conf = new SparkConf().setMaster("local").setAppName("ll");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> list = Arrays.asList(1,2,3,4,5,6);
        JavaRDD<Integer> parallelize = sc.parallelize(list);
        JavaRDD<Integer> filter = parallelize.filter(new Function<Integer, Boolean>() {
            @Override
            public Boolean call(Integer v1) throws Exception {
                return v1%2 == 0;
            }
        });
        filter.foreach(new VoidFunction<Integer>(){
            @Override
            public void call(Integer integer) throws Exception {
                System.out.println("============================"+integer+"==========================");
            }
        });
        sc.close();
    }

    public static void flatMap(){
        SparkConf conf = new SparkConf().setMaster("local").setAppName("ll");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> line = sc.textFile("C:\\Users\\Administrator\\Desktop\\国和\\临时文件\\aaa.txt");

         JavaRDD<String> stringJavaRDD = line.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" "));
            }
        });
        stringJavaRDD.foreach(new VoidFunction<String>(){
            @Override
            public void call(String s) throws Exception {
                System.out.println("============================"+s+"==========================");
            }
        });
    }
    public static void groupbykey(){
        SparkConf conf = new SparkConf().setMaster("local").setAppName("ll");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Tuple2<String,Integer>> list = Arrays.asList(new Tuple2<String, Integer>("class1",80),
                new Tuple2<String, Integer>("class2",90),
                new Tuple2<String, Integer>("class1",70),
                new Tuple2<String, Integer>("class2",60)
                );
        JavaPairRDD<String, Integer> stringIntegerJavaPairRDD = sc.parallelizePairs(list);
        JavaPairRDD<String, Iterable<Integer>> stringIterableJavaPairRDD = stringIntegerJavaPairRDD.groupByKey();
        stringIterableJavaPairRDD.foreach(new VoidFunction<Tuple2<String, Iterable<Integer>>>(){
            @Override
            public void call(Tuple2<String, Iterable<Integer>> tuple) throws Exception {
                System.out.println(tuple._1+":::"+tuple._2.toString());
                Iterator<Integer> iterator = tuple._2.iterator();
               /* if(iterator.hasNext()){
                    Integer next = iterator.next();
                }*/
            }
        });
        sc.close();
    }

    public static void reducebykey(){
        SparkConf conf = new SparkConf().setMaster("local").setAppName("ll");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Tuple2<String,Integer>> list = Arrays.asList(new Tuple2<String, Integer>("class1",80),
                new Tuple2<String, Integer>("class2",10),
                new Tuple2<String, Integer>("class1",70),
                new Tuple2<String, Integer>("class2",60)
        );
        JavaPairRDD<String, Integer> stringIntegerJavaPairRDD = sc.parallelizePairs(list);
        JavaPairRDD<String, Integer> stringIntegerJavaPairRDD1 = stringIntegerJavaPairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        stringIntegerJavaPairRDD1.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> tuple) throws Exception {
                System.out.println(tuple._1+"========="+tuple._2);
            }
        });
    }

    public static void  sortbykey() {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("ll");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Tuple2<String, Integer>> list = Arrays.asList(new Tuple2<String, Integer>("class1", 80),
                new Tuple2<String, Integer>("class2", 10),
                new Tuple2<String, Integer>("class1", 70),
                new Tuple2<String, Integer>("class2", 60),
                new Tuple2<String, Integer>("class1", 800),
                new Tuple2<String, Integer>("class2", 110),
                new Tuple2<String, Integer>("class1", 704),
                new Tuple2<String, Integer>("class2", 620)
        );
        JavaPairRDD<String, Integer> stringIntegerJavaPairRDD = sc.parallelizePairs(list);
        JavaPairRDD<String, Iterable<Integer>> stringIterableJavaPairRDD = stringIntegerJavaPairRDD.groupByKey();
        JavaRDD<Tuple2<String, List<Integer>>> map = stringIterableJavaPairRDD.map(new Function<Tuple2<String, Iterable<Integer>>, Tuple2<String, List<Integer>>>() {
            @Override
            public Tuple2<String, List<Integer>> call(Tuple2<String, Iterable<Integer>> v1) throws Exception {
                Iterator<Integer> iterator = v1._2.iterator();
                List aa = new ArrayList();
                while (iterator.hasNext()) {
                    aa.add(iterator.next());
                }
                Collections.sort(aa);
                return new Tuple2<String, List<Integer>>(v1._1, aa);
            }
        });


        map.foreach(new VoidFunction<Tuple2<String, List<Integer>>>() {
            @Override
            public void call(Tuple2<String, List<Integer>> o) throws Exception {
                String s = o._1;
                List<Integer> list1 = o._2;
                for (int aaa : list1) {
                    System.out.println("============================"+s+"==="+aaa+"=======================");
                }
            }
        });
    }


    public static void join(){
        SparkConf conf = new SparkConf().setMaster("local").setAppName("ll");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Tuple2<String, Integer>> list = Arrays.asList(new Tuple2<String, Integer>("class1", 80),
                new Tuple2<String, Integer>("class2", 10),
                new Tuple2<String, Integer>("class1", 70),
                new Tuple2<String, Integer>("class2", 60),
                new Tuple2<String, Integer>("class1", 800),
                new Tuple2<String, Integer>("class2", 110),
                new Tuple2<String, Integer>("class1", 704),
                new Tuple2<String, Integer>("class2", 620)
        );

        List<Tuple2<String, Integer>> list2 = Arrays.asList(new Tuple2<String, Integer>("class1", 8),
                new Tuple2<String, Integer>("class2", 1),
                new Tuple2<String, Integer>("class1", 7),
                new Tuple2<String, Integer>("class2", 6),
                new Tuple2<String, Integer>("class1", 8),
                new Tuple2<String, Integer>("class2", 1),
                new Tuple2<String, Integer>("class1", 7),
                new Tuple2<String, Integer>("class2", 6)
        );
        JavaPairRDD<String, Integer> stringIntegerJavaPairRDD = sc.parallelizePairs(list);
        JavaPairRDD<String, Integer> stringIntegerJavaPairRDD2 = sc.parallelizePairs(list2);
        JavaPairRDD<String, Tuple2<Integer, Integer>> join = stringIntegerJavaPairRDD.join(stringIntegerJavaPairRDD2);
        join.foreach(new VoidFunction<Tuple2<String, Tuple2<Integer, Integer>>>() {
            @Override
            public void call(Tuple2<String, Tuple2<Integer, Integer>> stringTuple2Tuple2) throws Exception {
                System.out.println(stringTuple2Tuple2._1+"====="+stringTuple2Tuple2._2._1+"----"+stringTuple2Tuple2._2._2);
            }
        });


        JavaPairRDD<String, Tuple2<Iterable<Integer>, Iterable<Integer>>> cogroup = stringIntegerJavaPairRDD.cogroup(stringIntegerJavaPairRDD2);
        cogroup.foreach(new VoidFunction<Tuple2<String, Tuple2<Iterable<Integer>, Iterable<Integer>>>>() {
            @Override
            public void call(Tuple2<String, Tuple2<Iterable<Integer>, Iterable<Integer>>> tup) throws Exception {
                String s = tup._1;
                Iterator<Integer> i1 = tup._2._1.iterator();
                Iterator<Integer> i2 = tup._2._2.iterator();
                while(i1.hasNext()){
                    System.out.println( s+"==="+i1.next());
                }
                while(i2.hasNext()){
                    System.out.println( s+"==="+i2.next());
                }
                System.out.println("=====================================================");
            }
        });

    }

    public static void reduce(){
        SparkConf conf = new SparkConf().setMaster("local").setAppName("ll");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> ll = Arrays.asList(1,2,3,4,5,6,7,8,9);
        JavaRDD<Integer> parallelize = sc.parallelize(ll);
        Integer reduce = parallelize.reduce(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        System.out.println("============================"+reduce+"==========================");
        sc.close();
    }

    public static void  collect(){
        SparkConf conf = new SparkConf().setMaster("local").setAppName("ll");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> ll = Arrays.asList(1,2,3,4,5,6,7,8,9);
        JavaRDD<Integer> parallelize = sc.parallelize(ll);
        JavaRDD<Integer> map = parallelize.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer v1) throws Exception {
                return v1 * 2;
            }
        });
        List<Integer> collect = map.collect();
        for(Integer  in : collect){
            System.out.println("============================"+in+"==========================");
        }
        sc.close();
    }

    public static void  count(){
        SparkConf conf = new SparkConf().setMaster("local").setAppName("ll");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> ll = Arrays.asList(1,2,3,4,5,6,7,8,9);
        JavaRDD<Integer> parallelize = sc.parallelize(ll);
        long count = parallelize.count();
        System.out.println("============================"+count+"==========================");
        sc.close();
    }

    public static void   countbykey(){
        SparkConf conf = new SparkConf().setMaster("local").setAppName("ll");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Tuple2<String,Integer>> list = Arrays.asList(new Tuple2<String, Integer>("class1",80),
                new Tuple2<String, Integer>("class2",10),
                new Tuple2<String, Integer>("class1",70),
                new Tuple2<String, Integer>("class2",60)
        );
        JavaPairRDD<String, Integer> stringIntegerJavaPairRDD = sc.parallelizePairs(list);
        long start = System.currentTimeMillis();
        Map<String, Object> stringObjectMap = stringIntegerJavaPairRDD.countByKey();
        long end = System.currentTimeMillis();
        System.out.println(end-start+"===========");

        long start1 = System.currentTimeMillis();
        Map<String, Object> stringObjectMap1 = stringIntegerJavaPairRDD.countByKey();
        long end1 = System.currentTimeMillis();
        System.out.println(end1-start1+"===========");


        Set<Map.Entry<String, Object>> entries = stringObjectMap.entrySet();
        for(Map.Entry<String, Object> ee :  entries){
            String key = ee.getKey();
            Object value = ee.getValue();
            Long aa = (Long)value;
            System.out.println(key+"====="+aa);
        }
    }

    public static void broadcast(){
        SparkConf conf = new SparkConf().setMaster("local").setAppName("ll");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> ll = Arrays.asList(1,2,3,4,5,6,7,8);
        final int fast =3;
        final Broadcast<Integer> broadcast = sc.broadcast(fast);
        JavaRDD<Integer> parallelize = sc.parallelize(ll);
        JavaRDD<Integer> map = parallelize.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer v1) throws Exception {
                Integer value = broadcast.value();
                return v1 * value;
            }
        });
        map.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer v1) throws Exception {
                System.out.println(v1+"===============");
            }
        });
    }

    public static void accumulatorVariable(){
        SparkConf conf = new SparkConf().setMaster("local[10]").setAppName("ll");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> ll = Arrays.asList(
                1,2,3,4,5,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,1,2,3,4,5,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,1,2,3,4,5,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,1,2,3,4,5,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,1,2,3,4,5,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,1,2,3,4,5,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,1,2,3,4,5,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,1,2,3,4,5,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,1,2,3,4,5,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,1,2,3,4,5,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,1,2,3,4,5,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,1,2,3,4,5,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,1,2,3,4,5,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,1,2,3,4,5,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,1,2,3,4,5,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,1,2,3,4,5,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,1,2,3,4,5,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,1,2,3,4,5,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,1,2,3,4,5,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,1,2,3,4,5,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,1,2,3,4,5,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,1,2,3,4,5,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,1,2,3,4,5,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,1,2,3,4,5,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,1,2,3,4,5,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,1,2,3,4,5,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,1,2,3,4,5,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,1,2,3,4,5,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,1,2,3,4,5,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,1,2,3,4,5,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,1,2,3,4,5,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,1,2,3,4,5,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,1,2,3,4,5,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,1,2,3,4,5,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,1,2,3,4,5,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,1,2,3,4,5,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,1,2,3,4,5,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,1,2,3,4,5,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,1,2,3,4,5,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,1,2,3,4,5,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36
        ,1,2,3,4,5,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,1,2,3,4,5,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,1,2,3,4,5,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,1,2,3,4,5,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,1,2,3,4,5,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,1,2,3,4,5,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,1,2,3,4,5,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,1,2,3,4,5,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,1,2,3,4,5,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,1,2,3,4,5,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,1,2,3,4,5,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,1,2,3,4,5,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,1,2,3,4,5,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,1,2,3,4,5,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,1,2,3,4,5,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,1,2,3,4,5,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,1,2,3,4,5,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,1,2,3,4,5,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,1,2,3,4,5,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,1,2,3,4,5,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,1,2,3,4,5,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,1,2,3,4,5,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,1,2,3,4,5,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,1,2,3,4,5,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,1,2,3,4,5,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,1,2,3,4,5,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,1,2,3,4,5,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,1,2,3,4,5,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,1,2,3,4,5,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,1,2,3,4,5,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,1,2,3,4,5,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,1,2,3,4,5,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,1,2,3,4,5,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,1,2,3,4,5,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,1,2,3,4,5,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,1,2,3,4,5,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,1,2,3,4,5,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,1,2,3,4,5,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,1,2,3,4,5,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,1,2,3,4,5,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36
        ,1,2,3,4,5,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,1,2,3,4,5,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,1,2,3,4,5,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,1,2,3,4,5,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,1,2,3,4,5,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,1,2,3,4,5,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,1,2,3,4,5,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,1,2,3,4,5,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,1,2,3,4,5,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,1,2,3,4,5,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,1,2,3,4,5,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,1,2,3,4,5,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,1,2,3,4,5,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,1,2,3,4,5,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,1,2,3,4,5,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,1,2,3,4,5,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,1,2,3,4,5,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,1,2,3,4,5,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,1,2,3,4,5,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,1,2,3,4,5,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,1,2,3,4,5,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,1,2,3,4,5,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,1,2,3,4,5,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,1,2,3,4,5,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,1,2,3,4,5,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,1,2,3,4,5,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,1,2,3,4,5,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,1,2,3,4,5,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,1,2,3,4,5,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,1,2,3,4,5,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,1,2,3,4,5,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,1,2,3,4,5,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,1,2,3,4,5,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,1,2,3,4,5,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,1,2,3,4,5,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,1,2,3,4,5,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,1,2,3,4,5,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,1,2,3,4,5,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,1,2,3,4,5,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,1,2,3,4,5,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36
        );
        final Accumulator<Integer> sum = sc.accumulator(0);
        JavaRDD<Integer> parallelize = sc.parallelize(ll,10);
        parallelize.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
               sum.add(integer);
            }
        });//79200
        System.out.println(sum.value());
    }

    public static void px(){
        SparkConf conf = new SparkConf().setMaster("local").setAppName("ll");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> line = sc.textFile("C:\\Users\\Administrator\\Desktop\\国和\\临时文件\\aaa.txt");
        JavaRDD<String> objectJavaRDD = line.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" "));
            }
        });
        JavaPairRDD<String, Integer> stringIntegerJavaPairRDD = objectJavaRDD.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        });
        JavaPairRDD<String, Integer> stringIntegerJavaPairRDD1 = stringIntegerJavaPairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        JavaPairRDD<Integer, String> integerStringJavaPairRDD = stringIntegerJavaPairRDD1.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> vv) throws Exception {
                return new Tuple2<Integer, String>(vv._2, vv._1);
            }
        });
        JavaPairRDD<Integer, String> integerStringJavaPairRDD1 = integerStringJavaPairRDD.sortByKey(false);
        JavaPairRDD<String, Integer> stringIntegerJavaPairRDD2 = integerStringJavaPairRDD1.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<Integer, String> vv) throws Exception {
                return new Tuple2<String, Integer>(vv._2, vv._1);
            }
        });
        stringIntegerJavaPairRDD2.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> vv) throws Exception {
                System.out.println(vv._1+"========"+vv._2);
            }
        });
        sc.close();
    }

    public static void zdypx(){
        SparkConf conf = new SparkConf().setMaster("local").setAppName("ll");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> line = sc.textFile("C:\\Users\\Administrator\\Desktop\\国和\\临时文件\\bbb.txt");
        JavaPairRDD<SecondarySortKey, String> secondarySortKeyStringJavaPairRDD = line.mapToPair(new PairFunction<String, SecondarySortKey, String>() {
            @Override
            public Tuple2<SecondarySortKey, String> call(String s) throws Exception {
                String[] split = s.split(" ");
                SecondarySortKey kk = new SecondarySortKey(Integer.valueOf(split[0]), Integer.valueOf(split[1]));
                return new Tuple2<SecondarySortKey, String>(kk, s);
            }
        });
        JavaPairRDD<SecondarySortKey, String> secondarySortKeyStringJavaPairRDD1 = secondarySortKeyStringJavaPairRDD.sortByKey(false);
        JavaRDD<String> map = secondarySortKeyStringJavaPairRDD1.map(new Function<Tuple2<SecondarySortKey, String>, String>() {
            @Override
            public String call(Tuple2<SecondarySortKey, String> v1) throws Exception {
                return v1._2;
            }
        });
        map.foreach(new VoidFunction<String>() {
            @Override
            public void call(String o) throws Exception {
                System.out.println("============================"+o+"==========================");
            }
        });
    }

    public static void fztop3(){
        SparkConf conf = new SparkConf().setMaster("local").setAppName("ll");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> line = sc.textFile("C:\\Users\\Administrator\\Desktop\\国和\\临时文件\\ccc.txt");
        JavaPairRDD<String, Integer> stringIntegerJavaPairRDD = line.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                String[] split = s.split(" ");
                return new Tuple2<String, Integer>(split[0], Integer.valueOf(split[1]));
            }
        });
        JavaPairRDD<String, Iterable<Integer>> stringIterableJavaPairRDD = stringIntegerJavaPairRDD.groupByKey();
        JavaPairRDD<String, Iterable<Integer>> objectObjectJavaPairRDD = stringIterableJavaPairRDD.mapToPair(new PairFunction<Tuple2<String, Iterable<Integer>>, String, Iterable<Integer>>() {
            @Override
            public Tuple2<String, Iterable<Integer>> call(Tuple2<String, Iterable<Integer>> classSource) throws Exception {
                Integer[] top3 = new Integer[3];
                String className =classSource._1;
                Iterator<Integer> iterator = classSource._2.iterator();
                while(iterator.hasNext()){
                    Integer score = iterator.next();
                    for(int i=0 ;i<3; i++){
                        if(top3[i]==null){
                            top3[i]=score;
                            break;
                        }
                        else if(top3[i]<score){
                            top3[i]=score;
                        }
                    }
                }
                return new Tuple2<String, Iterable<Integer>>(className,Arrays.asList(top3));
            }
        });
        objectObjectJavaPairRDD.foreach(new VoidFunction<Tuple2<String, Iterable<Integer>>>() {
            @Override
            public void call(Tuple2<String, Iterable<Integer>> tuple) throws Exception {
                Iterator<Integer> iterator = tuple._2.iterator();
                String fs = "";
                while (iterator.hasNext()) fs = fs+iterator.next()+"==";
                System.out.println(tuple._1+"=="+fs);
            }
        });
    }
}
