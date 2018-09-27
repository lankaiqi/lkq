package textSpark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;

/**
 * Created by Administrator on 2018/9/12.
 */
public class HiceSjy {
    public static void main(String[] args) {
        first();
    }

    public static void first() {
        SparkConf conf = new SparkConf().setAppName("ll");
        JavaSparkContext sc = new JavaSparkContext(conf);
        HiveContext hiveContext = new HiveContext(sc.sc());

        hiveContext.sql("DROP TABLE IF EXISTS student_infos");

        hiveContext.sql("CREATE TABLE IF NOT EXISTS student_infos (name STRING, age INT)");

        hiveContext.sql("LOAD DATA LOCAL INPATH '/home/hadoop/logCs/sparkJC/student_indfos.txt' INTO TABLE student_infos");

        hiveContext.sql("DROP TABLE IF EXISTS student_scores");

        hiveContext.sql("CREATE TABLE IF NOT EXISTS student_scores (name STRING, score INT)");

        hiveContext.sql("LOAD DATA LOCAL INPATH '/home/hadoop/logCs/sparkJC/student_scores.txt' INTO TABLE student_scores");

        DataFrame goodStudentDF = hiveContext.sql("SELECT si.name ,si.age ,ss.score " +
                "FROM student_infos si " +
                "JOIN student_scores ss ON si.name=ss.name " +
                "WHERE ss.score>=80");

        hiveContext.sql("DROP TABLE IF EXISTS good_student_indfos");
        goodStudentDF.saveAsTable("good_student_indfos");
        //可用用table方法,针对hive表,直接创建DataFrame
        Row[] goodstudentRows = hiveContext.table("good_student_indfos").collect();
        for(Row goodstudentRow : goodstudentRows){
            System.out.println(goodstudentRow);
        }

        sc.close();
    }
}
