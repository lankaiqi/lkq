package textSpark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Administrator on 2018/9/15.
 */
public class top3 {
    public static void main(String[] args) {
       first();
       //cs();
    }
    public static void cs(){
        SparkConf conf = new SparkConf().setAppName("ll");
        JavaSparkContext sc = new JavaSparkContext(conf);
        HiveContext hiveContext = new HiveContext(sc.sc());
        JavaRDD<String> lines  = sc.textFile("/sole.txt");
        List<String> collect = lines.collect();
        for(String ss : collect){
            System.out.println("============================"+ss+"==========================");
        }
    }
    public static void first(){
        SparkConf conf = new SparkConf().setAppName("ll");
        JavaSparkContext sc = new JavaSparkContext(conf);
        HiveContext hiveContext = new HiveContext(sc.sc());

        JavaRDD<String> lines  = sc.textFile("/sole.txt");
        //JavaRDD<String> lines  = sc.textFile("file:///home/hadoop/logCs/sparkJC/sole.txt");
        //JavaRDD<String> lines  = sc.textFile("C://Users//Administrator//Desktop//国和//临时文件//linux//sole.txt");
        JavaRDD<Row> map = lines.map(new Function<String, Row>() {
            @Override
            public Row call(String v1) throws Exception {
                String[] split = v1.split(" ");//0 date 1  sole   2  id
                return RowFactory.create(split[0]+"_"+split[1]+"_"+split[2]  ,split[0], split[1], split[2],  split[0]+"_"+split[1]);
            }
        });

        ArrayList<StructField> structFields = new ArrayList<StructField>();
        structFields.add(DataTypes.createStructField("jh",DataTypes.StringType,true));
        structFields.add(DataTypes.createStructField("date",DataTypes.StringType,true));
        structFields.add(DataTypes.createStructField("sole",DataTypes.StringType,true));
        structFields.add(DataTypes.createStructField("id",DataTypes.StringType,true));
        structFields.add(DataTypes.createStructField("datesole",DataTypes.StringType,true));
        StructType structType = DataTypes.createStructType(structFields);
        DataFrame studentDF = hiveContext.createDataFrame(map, structType);
        studentDF.registerTempTable("uvtj");

        DataFrame sql = hiveContext.sql("select distinct(jh),date,sole,id,datesole from uvtj");
        JavaRDD<Row> rowJavaRDD = sql.javaRDD();//得到数据
        List<Row> collect1 = rowJavaRDD.collect();
        for(Row ss : collect1){
            System.out.println("1111============================"+ss+"==========================");
        }

        ArrayList<StructField> structFields2 = new ArrayList<StructField>();
        structFields2.add(DataTypes.createStructField("jh",DataTypes.StringType,true));
        structFields2.add(DataTypes.createStructField("date",DataTypes.StringType,true));
        structFields2.add(DataTypes.createStructField("sole",DataTypes.StringType,true));
        structFields2.add(DataTypes.createStructField("id",DataTypes.StringType,true));
        structFields2.add(DataTypes.createStructField("datesole",DataTypes.StringType,true));
        StructType structType2 = DataTypes.createStructType(structFields);
        DataFrame studentDFQC = hiveContext.createDataFrame(rowJavaRDD, structType2);
        studentDFQC.registerTempTable("uvtjqc");

        DataFrame sqlcount = hiveContext.sql("select count(jh) count1 ,datesole from uvtjqc group by datesole");
        JavaRDD<Row> rowJavaRDDCount = sqlcount.javaRDD();//count(jh),datesole
        JavaRDD<Row> countCF = rowJavaRDDCount.map(new Function<Row, Row>() {
            @Override
            public Row call(Row v1) throws Exception {
                long count = v1.getLong(0);
                String[] split = v1.getString(1).split("_");
                return RowFactory.create(count, split[0], split[1]);
            }
        });
        List<Row> collect2 = countCF.collect();
        for(Row ss : collect2){
            System.out.println("22222============================"+ss+"==========================");
        }


        ArrayList<StructField> structFieldCount = new ArrayList<StructField>();
        structFieldCount.add(DataTypes.createStructField("count1",DataTypes.LongType,true));
        structFieldCount.add(DataTypes.createStructField("date",DataTypes.StringType,true));
        structFieldCount.add(DataTypes.createStructField("sole",DataTypes.StringType,true));
        StructType structTypeCount = DataTypes.createStructType(structFieldCount);
        DataFrame studentDFCount = hiveContext.createDataFrame(countCF, structTypeCount);
        studentDFCount.registerTempTable("uvtjcount");

        DataFrame top33 = hiveContext.sql("select date from uvtjcount");
        List<Row> collect = top33.javaRDD().collect();
        for (Row rr : collect) System.out.println("============================"+rr+"==========================");


        String sqll = "select count1,date,sole,rank from " +
                "( select count1,date,sole, " +
                "row_number() OVER (PARTITION BY date ORDER BY count1 DESC) rank " +
                "from uvtjcount ) abc WHERE rank<=3";
        System.out.println(sql);
        DataFrame top3 = hiveContext.sql(sqll);
        hiveContext.sql("DROP TABLE IF EXISTS top3sole");
        top3.saveAsTable("top3sole");
        sc.close();
    }
}
