package textSpark.al2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.hive.HiveContext;

import java.math.BigDecimal;

/**
 * Created by Administrator on 2018/9/22.
 */
public class SparkPvUv {
    public static void main(String[] args) {
        String yesterday = "2016-02-20";

        SparkConf conf = new SparkConf().setAppName("ll");
        JavaSparkContext sc = new JavaSparkContext(conf);
        HiveContext hiveContext = new HiveContext(sc.sc());
        // 开发第一个关键指标：页面pv统计以及排序
        calculateDailyPagePv(hiveContext, yesterday);
        // 开发第二个关键指标：页面uv统计以及排序
        calculateDailyPageUv(hiveContext, yesterday);
        // 开发第三个关键指标：新用户注册比率统计
        calculateDailyNewUserRegisterRate(hiveContext, yesterday);
        // 开发第四个关键指标：用户跳出率统计
        calculateDailyUserJumpRate(hiveContext, yesterday);
        // 开发第五个关键指标：版块热度排行榜
        calculateDailySectionPvSort(hiveContext, yesterday);
    }

    public static void calculateDailyPagePv(HiveContext hiveContext,String yesterday ){
        String sql = "select date, " +
                "pageid, " +
                "pv " +
                "from( " +
                "select date, " +
                "pageid, " +
                "count(*) pv " +
                "from news_access " +
                "where action='view' " +
                "and date= '"+yesterday+"' " +
                "group by date,pageid " +
                ") t " +
                "order by pv desc ";
        DataFrame df = hiveContext.sql(sql);
        df.show();
    }

    public static void calculateDailyPageUv(HiveContext hiveContext,String yesterday ){
        String sql = "select " +
                "date, " +
                "pageid, " +
                "pv " +
                "from ( " +
                "select date," +
                "pageid," +
                "count(*) pv " +
                "from(  " +
                "select date, " +
                "pageid, " +
                "userid " +
                "from news_access " +
                "where action='view' and date= '2016-02-20' group by date,pageid ,userid" +
                ") t " +
                "group by  date,pageid) a " +
                "order by pv desc";
        DataFrame df = hiveContext.sql(sql);
        df.show();
    }

    public static void calculateDailyNewUserRegisterRate(HiveContext hiveContext,String yesterday ){
        String sql1 = "select count(1) from  news_access where  action='view'  and date= '2016-02-20' and userid is null";
        String sql2 ="select count(1) from  news_access where  action='register'  and date= '2016-02-20'";
        DataFrame df1 = hiveContext.sql(sql1);
        DataFrame df2 = hiveContext.sql(sql2);

        Object o1 = df1.collect()[0].get(0);
        Object o2 = df2.collect()[0].get(0);
        long long1=0L;
        long long2=0L;
        if(o1 != null)  long1 = Long.valueOf(String.valueOf(o1));
        if(o2  != null) long2 = Long.valueOf(String.valueOf(o2));

        double rate = (double)long2 / (double)long1;
        System.out.println("===开发第三个关键指标：新用户注册比率统计===="+formatDouble(rate,2)+"==========================");
    }

    public static void  calculateDailyUserJumpRate(HiveContext hiveContext,String yesterday ){
        String sql1 = "select count(1) from  news_access where  action='view'  and date= '2016-02-20' and userid is not null";
        String sql2 ="select count(1) from (select count(1) cnt  from  news_access where  action='view'  and date= '2016-02-20' and userid is not null group by userid HAVING cnt=1) a";
        DataFrame df1 = hiveContext.sql(sql1);
        DataFrame df2 = hiveContext.sql(sql2);

        Object o1 = df1.collect()[0].get(0);
        Object o2 = df2.collect()[0].get(0);
        long long1=0L;
        long long2=0L;
        if(o1 != null)  long1 = Long.valueOf(String.valueOf(o1));
        if(o2  != null) long2 = Long.valueOf(String.valueOf(o2));

        double rate = (double)long2 / (double)long1;
        System.out.println("===开发第四个关键指标：用户跳出率统计===="+formatDouble(rate,2)+"==========================");
    }

    public static void calculateDailySectionPvSort(HiveContext hiveContext,String yesterday ){
        String sql = "select date, " +
                "section, " +
                "pv " +
                "from( " +
                "select date, " +
                "section, " +
                "count(*) pv " +
                "from news_access " +
                "where action='view' " +
                "and date= '"+yesterday+"' " +
                "group by date,section " +
                ") t " +
                "order by pv desc ";
        DataFrame df = hiveContext.sql(sql);
        df.show();
    }

    /**
     * 格式化小数
     * @param scale 四舍五入的位数
     * @return 格式化小数
     */
    private static double formatDouble(double num, int scale) {
        BigDecimal bd = new BigDecimal(num);
        return bd.setScale(scale, BigDecimal.ROUND_HALF_UP).doubleValue();
    }

}
