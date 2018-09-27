package textSpark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2018/8/30.
  */
object LineCounta {

  def main(args: Array[String]): Unit = {
    //ff1();
    map();
  }

  def ff1(): Unit ={
    val conf = new SparkConf().setMaster("local").setAppName("ll");
    val sc = new SparkContext(conf);

    val lines = sc.textFile("C:\\Users\\Administrator\\Desktop\\国和\\临时文件\\aaa.txt",1);

    val lintAndOne = lines.map{ lines=>(lines,1) }

    val lintCount: RDD[(String, Int)] = lintAndOne.reduceByKey(_+_)
    lintCount.foreach(ll=>println(ll._1+"========="+ll._2));
  }
  def map(): Unit ={
    val conf = new SparkConf().setMaster("local").setAppName("ll");
    val sc = new SparkContext(conf);
    val lines = sc.textFile("C:\\Users\\Administrator\\Desktop\\国和\\临时文件\\aaa.txt",1);
    /*val array = Array("hello you","hello tom");
    val numRDD = sc.parallelize(array,1);*/

    val flatRDD = lines.flatMap(x=>x.split(" "));

    flatRDD.foreach(x=>println(x))
  }
}
