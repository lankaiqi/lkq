package textSpark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Administrator on 2018/9/13.
 */
public class JdbcDataSource {
    public static void main(String[] args) {

        //first();
        kchz();
    }

    public static void first(){
        SparkConf conf = new SparkConf().setAppName("ll").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);
        Map<String,String> options = new HashMap<String,String>();
        options.put("url","jdbc:mysql://lkq:3306/test");
        options.put("dbtable","student_infos");
        options.put("user","root");
        options.put("password","root");
        DataFrame studentInfosDF = sqlContext.read().format("jdbc").options(options).load();

        options.put("dbtable","student_scores");
        DataFrame studentScoresDF = sqlContext.read().format("jdbc").options(options).load();

        JavaPairRDD<String, Tuple2<Integer, Integer>> join = studentInfosDF.javaRDD().mapToPair(new PairFunction<Row, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Row row) throws Exception {
                return new Tuple2<String, Integer>(row.getString(0), Integer.valueOf(String.valueOf(row.getInt(1))));
            }
        }).join(studentScoresDF.javaRDD().mapToPair(new PairFunction<Row, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Row row) throws Exception {
                return new Tuple2<String, Integer>(row.getString(0), Integer.valueOf(String.valueOf(row.getInt(1))));
            }
        }));

        JavaRDD<Row> studentsRowsRDD = join.map(new Function<Tuple2<String, Tuple2<Integer, Integer>>, Row>() {
            @Override
            public Row call(Tuple2<String, Tuple2<Integer, Integer>> v1) throws Exception {
                return RowFactory.create(v1._1, v1._2._1, v1._2._2);
            }
        });

        JavaRDD<Row> filter = studentsRowsRDD.filter(new Function<Row, Boolean>() {
            @Override
            public Boolean call(Row v1) throws Exception {
                if (v1.getInt(2) > 80) return true;
                else return false;
            }
        });

        ArrayList<StructField> structFields = new ArrayList<StructField>();
        structFields.add(DataTypes.createStructField("name", DataTypes.StringType,true));
        structFields.add(DataTypes.createStructField("age", DataTypes.IntegerType,true));
        structFields.add(DataTypes.createStructField("score", DataTypes.IntegerType,true));
        StructType structType = DataTypes.createStructType(structFields);
        DataFrame studentDF = sqlContext.createDataFrame(filter, structType);

        options.put("dbtable","good_student_infos");
        studentDF.javaRDD().foreach(new VoidFunction<Row>() {
            @Override
            public void call(Row row) throws Exception {
                String cs = "'"+String .valueOf(row.getString(0))+"',"+Integer .valueOf(row.getInt(1))+","+Integer .valueOf(row.getInt(2));
                String sql ="insert into good_student_infos values("+cs+")";
                Class.forName("com.mysql.jdbc.Driver");
                Connection conn = null;
                Statement stmt = null;
                try {
                    conn = DriverManager.getConnection("jdbc:mysql://lkq:3306/test","root","root");
                    stmt=conn.createStatement();
                    System.out.println("============================"+sql+"==========================");
                    stmt.executeUpdate(sql);
                } catch (SQLException e) {
                    e.printStackTrace();
                } finally {
                    if(stmt != null ) stmt.close();
                    if(conn != null ) conn.close();
                }

            }
        });
    }
    public static void kchz() {
        SparkConf conf = new SparkConf().setAppName("ll");
        JavaSparkContext sc = new JavaSparkContext(conf);
        HiveContext hiveCintext = new HiveContext(sc.sc());

        hiveCintext.sql("DROP TABLE IF EXISTS sales");
        hiveCintext.sql("CREATE TABLE IF NOT EXISTS sales (" +
                "product STRING," +
                "category STRING," +
                "revenue BIGINT)");
        hiveCintext.sql("LOAD DATA " +
                "LOCAL INPATH '/home/hadoop/logCs/sparkJC/product.txt' " +
                "INTO TABLE sales");

        DataFrame top3DF = hiveCintext.sql("" +
                "SELECT product,category,revenue " +
                "FROM ( " +
                "SELECT " +
                "product," +
                "category," +
                "revenue," +
                "row_number() OVER (PARTITION BY category ORDER BY revenue DESC) rank " +
                "FROM sales " +
                ") tmp_sales " +
                "WHERE rank<=3");
        hiveCintext.sql("DROP TABLE IF EXISTS top3_sales");
        top3DF.saveAsTable("top3_sales");
        sc.close();
    }
}
