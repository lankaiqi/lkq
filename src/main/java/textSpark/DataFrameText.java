package textSpark;

import org.apache.spark.Partition;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;
import textSpark.model.Student;

import java.util.*;

/**
 * Created by Administrator on 2018/9/11.
 */
public class DataFrameText {
    public static void main(String[] args) {
        //firsh();
        //fs();
        //dtzdysj();
        //loadSave();
        //ljcz();
        //ljcz2();
       // mapPartitions();
       // mapPartitionsWithIndex();//带索引
        //sample();//样品
        //union();//联盟
        //intersection();//交叉
        //distinct();//不同的
        aggregateByKey();//聚合
    }
    public static void firsh(){
        //创建DataFrame
        SparkConf conf = new SparkConf().setAppName("DataFrame").setMaster("aaaa");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);
        //创建的DataFrame理解为一张表
        DataFrame df = sqlContext.read().json("C:\\Users\\Administrator\\Desktop\\国和\\临时文件\\ddd.txt");
        //打印DataFrame中所有数据信息
        df.show();
        //打印DataFrame的元数据信息
        df.printSchema();
        //查询某列所有数据
        df.select("name").show();
        //查询某几列所有数据,并对列进行计算
        df.select(df.col("name"),df.col("age").plus(1)).show();
        //根据某一列的值进行过滤
        df.filter(df.col("age").gt(18)).show();
        //根据某一列进行分组
        df.groupBy(df.col("age")).count().show();
    }




    public static void fs(){
        SparkConf conf = new SparkConf().setAppName("DataFrame").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);
        JavaRDD<String> lines = sc.textFile("C:\\Users\\Administrator\\Desktop\\国和\\临时文件\\eee.txt");
        JavaRDD<Student> map = lines.map(new Function<String, Student>() {
            @Override
            public Student call(String v1) throws Exception {
                String[] split = v1.split(",");
                Student ss = new Student(Integer.valueOf(split[0]), split[1], Integer.valueOf(split[2]));
                return ss;
            }
        });
        DataFrame dataFrameDF = sqlContext.createDataFrame(map,Student.class);
        dataFrameDF.registerTempTable("stydents");
        DataFrame sql = sqlContext.sql("select * from stydents where age<=18");
        JavaRDD<Row> rowJavaRDD = sql.javaRDD();
        JavaRDD<Student> map1 = rowJavaRDD.map(new Function<Row, Student>() {
            @Override
            public Student call(Row v1) throws Exception {

                Student ss = new Student(Integer.valueOf(v1.getInt(0)), v1.getString(2),v1.getInt(1));
                return ss;
            }
        });
        List<Student> collect = map1.collect();
        for(Student ss :collect){
            System.out.println(ss.toString());
        }
    }






    public static void dtzdysj(){
        SparkConf conf = new SparkConf().setAppName("DataFrame").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);
        JavaRDD<String> lines  = sc.textFile("C:\\Users\\Administrator\\Desktop\\国和\\临时文件\\eee.txt");
        JavaRDD<Row> map = lines.map(new Function<String, Row>() {
            @Override
            public Row call(String v1) throws Exception {
                String[] split = v1.split(",");
                return RowFactory.create(Integer.valueOf(split[0]), split[1], Integer.valueOf(split[2]));
            }
        });
        ArrayList<StructField> structFields = new ArrayList<StructField>();
        structFields.add(DataTypes.createStructField("id",DataTypes.IntegerType,true));
        structFields.add(DataTypes.createStructField("name",DataTypes.StringType,true));
        structFields.add(DataTypes.createStructField("age",DataTypes.IntegerType,true));
        StructType structType = DataTypes.createStructType(structFields);
        DataFrame studentDF = sqlContext.createDataFrame(map, structType);
        studentDF.registerTempTable("students");

        DataFrame sql = sqlContext.sql("select * from students where age<=18");

        List<Row> collect = sql.javaRDD().collect();
       for(Row rr :collect){
           System.out.println("============================"+rr+"==========================");
       }
    }


    public static void loadSave(){
        SparkConf conf = new SparkConf().setAppName("DataFrame").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);
      //  DataFrame userDF = sqlContext.read().format("parquet").load("C:\\Users\\Administrator\\Desktop\\国和\\临时文件\\ggg.txt");
       // DataFrame userDF = sqlContext.read().parquet("C:\\Users\\Administrator\\Desktop\\国和\\临时文件\\hhh.txt");
        DataFrame userDF = sqlContext.read().parquet("C:\\Users\\Administrator\\Desktop\\国和\\临时文件\\aaa\\gender=male\\count=us\\aaa.parquet");
       // userDF.registerTempTable("student");
       // DataFrame sql = sqlContext.sql("select name form students where age<=18");
        //sql.javaRDD().map()
        userDF.printSchema();
        userDF.show();

       // userDF.write().format("parquet").save("C:\\Users\\Administrator\\Desktop\\国和\\临时文件\\ggg.txt");

    }


    public static void ljcz(){
        SparkConf conf = new SparkConf().setAppName("DataFrame").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        final List<Integer> numbers = Arrays.asList(1,2,3,4,5);
        JavaRDD<Integer> parallelize = sc.parallelize(numbers);
        final List<Integer> ll = new ArrayList<Integer>();
        ll.add(0);
        parallelize.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                Integer iii = ll.get(0);
                iii+=integer;
                ll.add(0,iii);
                System.out.println("====111========================"+ll.get(0)+"==========================");
            }
        });
        System.out.println("============================"+ll.get(0)+"==========================");
    sc.close();
    }

    public static void ljcz2(){
        SparkConf conf = new SparkConf().setAppName("DataFrame").set("spark.default.parallelism","2").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        final List<Integer> numbers = Arrays.asList(1,2,3,4,5);
        JavaRDD<Integer> parallelize = sc.parallelize(numbers);
        final List<Integer> ll = new ArrayList<Integer>();
        ll.add(0);
        parallelize.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                Integer iii = ll.get(0);
                iii+=integer;
                ll.add(0,iii);
                System.out.println("====111========================"+ll.get(0)+"==========================");
            }
        });
        System.out.println("============================"+ll.get(0)+"==========================");
        sc.close();
    }

    public static void mapPartitions(){
        SparkConf conf = new SparkConf().setAppName("DataFrame").set("spark.default.parallelism","2").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<String> students = Arrays.asList("张三","李四","王二","赵五");
        JavaRDD<String> parallelize = sc.parallelize(students, 2);
        final Map<String,Double> map = new HashMap<String,Double>();
        map.put("张三",100.01);
        map.put("李四",110.34);
        map.put("王二",232.67);
        map.put("赵五",666.88);

        JavaRDD<Double> integerJavaRDD = parallelize.mapPartitions(new FlatMapFunction<Iterator<String>, Double>() {
            @Override
            public Iterable<Double> call(Iterator<String> stringIterator) throws Exception {
                List<Double> lll = new ArrayList<Double>();
                while(stringIterator.hasNext()){
                    String next = stringIterator.next();
                    Double aDouble = map.get(next);
                    lll.add(aDouble);
                }
                return lll;
            }
        });
        for(Double dd  : integerJavaRDD.collect()){
            System.out.println("============================"+dd+"==========================");
        }
    }

    public static void  mapPartitionsWithIndex(){
        SparkConf conf = new SparkConf().setAppName("DataFrame").set("spark.default.parallelism","2").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<String> students = Arrays.asList("张三","李四","王二","赵五");
        JavaRDD<String> parallelize = sc.parallelize(students, 2);
        JavaRDD<String> stringJavaRDD = parallelize.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>() {
            @Override
            public Iterator<String> call(Integer v1, Iterator<String> v2) throws Exception {
                List<String> list = new ArrayList<String>();
                while (v2.hasNext()) {
                    String next = v2.next();
                    list.add(next + "=========" + v1);
                }
                return list.iterator();
            }
        }, true);
        List<String> collect = stringJavaRDD.collect();
        for(String ss :collect){
            System.out.println("============================"+ss+"==========================");
        }

    }

    public static void sample(){
        SparkConf conf = new SparkConf().setAppName("DataFrame").set("spark.default.parallelism","2").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<String> students = Arrays.asList("a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s");
        JavaRDD<String> parallelize = sc.parallelize(students, 2);
        JavaRDD<String> sample = parallelize.sample(false, 0.1);
        List<String> collect = sample.collect();
        for(String ss : collect){
            System.out.println("============================"+ss+"==========================");
        }
    }

    public static void union(){
        SparkConf conf = new SparkConf().setAppName("DataFrame").set("spark.default.parallelism","2").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<String> students1 = Arrays.asList("张三","李四","王二","赵五");
        List<String> students2 = Arrays.asList("张三2","李四2","王二2","赵五2");
        JavaRDD<String> parallelize1 = sc.parallelize(students1, 2);
        JavaRDD<String> parallelize2 = sc.parallelize(students2, 2);

        JavaRDD<String> union = parallelize1.union(parallelize2);
        for(String ss : union.collect()){
            System.out.println("============================"+ss+"==========================");
        }
    }
    public static void intersection(){
        SparkConf conf = new SparkConf().setAppName("DataFrame").set("spark.default.parallelism","2").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<String> students1 = Arrays.asList("张三","李四","王二","赵五");
        List<String> students2 = Arrays.asList("张三","李四2","王二2","赵五2");
        JavaRDD<String> parallelize1 = sc.parallelize(students1, 2);
        JavaRDD<String> parallelize2 = sc.parallelize(students2, 2);
        JavaRDD<String> intersection = parallelize1.intersection(parallelize2);
        for(String ss : intersection.collect()){
            System.out.println("============================"+ss+"==========================");
        }
    }

    public static void  distinct(){
        SparkConf conf = new SparkConf().setAppName("DataFrame").set("spark.default.parallelism","2").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<String> students1 = Arrays.asList("张三","张三","王二","赵五");
        JavaRDD<String> parallelize1 = sc.parallelize(students1, 2);
        JavaRDD<String> aaa = parallelize1.distinct();
        for(String ss : aaa.collect()){
            System.out.println("============================"+ss+"==========================");
        }
    }

    public static void   aggregateByKey(){
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
        JavaPairRDD<String, Integer> stringIntegerJavaPairRDD1 = stringIntegerJavaPairRDD.aggregateByKey(0, new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1+v2;
            }
        }, new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1+v2;
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
}
