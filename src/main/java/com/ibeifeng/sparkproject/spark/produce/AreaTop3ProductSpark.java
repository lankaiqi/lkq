package com.ibeifeng.sparkproject.spark.produce;

import com.alibaba.fastjson.JSONObject;
import com.ibeifeng.sparkproject.conf.ConfigurationManager;
import com.ibeifeng.sparkproject.constant.Constants;
import com.ibeifeng.sparkproject.dao.IAreaTop3ProductDAO;
import com.ibeifeng.sparkproject.dao.ITaskDao;
import com.ibeifeng.sparkproject.dao.factory.DAOFactory;
import com.ibeifeng.sparkproject.domain.AreaTop3Product;
import com.ibeifeng.sparkproject.domain.Task;
import com.ibeifeng.sparkproject.util.ParamUtils;
import com.ibeifeng.sparkproject.util.SparkUtils;
import org.apache.derby.iapi.store.raw.data.DataFactory;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.codehaus.janino.Java;
import scala.Tuple2;

import java.util.*;

/**
 * 各区域top3 热门商品统计spark作业
 */
public class AreaTop3ProductSpark {
    public static void main(String[] args){
        //TODO 创建sparkConf
        SparkConf conf = new SparkConf().setAppName(Constants.SPARK_APP_NAME_PRODUCT);
        SparkUtils.setMaster(conf);

        //TODO 构建spark上下文
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = SparkUtils.getSQLContext(sc.sc());
       // sqlContext.setConf("spark.sql.shuffle.partitions","1000");
       // sqlContext.setConf("spark.sql.autoBroadcastJoinThreshold","20971520");

        //TODO 注册自定义函数
        sqlContext.udf().register("concat_long_string",new ConcatLongStringUDF(),DataTypes.StringType);
        sqlContext.udf().register("group_concat_distinct",new GroupConcatDistinctUDAF());
        sqlContext.udf().register("get_json_object",new GetJsonObjectUDF(),DataTypes.StringType);
        sqlContext.udf().register("random_prefix",new RandomPrefixUDF(),DataTypes.StringType);
        sqlContext.udf().register("remove_random_prefix",new RemoveRandomPrefixUDF(),DataTypes.StringType);
        //TODO 准备模拟数据
        SparkUtils.mockData(sc,sqlContext);

        //TODO 获取命令行传入的taskid,查询对应的任务参数
        ITaskDao taskDAO = DAOFactory.getTaskDao();
        long taskid = ParamUtils.getTaskIdFromArgs(args,Constants.SPARK_LOCAL_TASKID_PRODUCT);
        Task task = taskDAO.findById(taskid);

        JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());
        String startDate = ParamUtils.getParam(taskParam,Constants.PARAM_START_DATE);
        String endDate = ParamUtils.getParam(taskParam,Constants.PARAM_END_DATE);

        //TODO 查询用户指定日期范围内的用户点击行为
        //TODO 技术点1 HIVE数据源使用
        JavaPairRDD<Long,Row> cityid2clickActionRDD = getCityid2ClickActionRDDByDate(sqlContext, startDate, endDate);

        //TODO 从mysql中查询城市信息
        //TODO 技术点2 异构数据源之mysql的使用
        JavaPairRDD<Long,Row> cityid2cityInfoRDD = getCityid2CityinfoRDD(sqlContext);

        //TODO 生成点击商品基础信息临时表
        //TODO 技术点3 将RDD转化为DataFrame,并注册临时表
        generateClickProductBasicTable(sqlContext,cityid2clickActionRDD,cityid2cityInfoRDD);

        //TODO 生成各区域各商品点击次数的临时表
        generateTempAreaProductClickCountTable(sqlContext);

        //TODO 生成包含完整商品信息的各区域各商品点击次数的临时表
        generateTempAreaFullProductClickCountTable(sqlContext);

        //TODO 使用开窗函数获取各个区域内点击次数排名前3的热门商品
        JavaRDD<Row> areaTop3ProductRDD = generateAreaTop3ProductRDD(sqlContext);

        //TODO 这边写入,mysql和之前不太一样,
        //因为实际上,就这个业务需求而言,计算出来的最终数据量是比较小的,
        //总共就不到10个区域,每个区域还是top热门商品,总共最后也就是几十个
        //所以就可以直接将数据collect()到本地,
        //用批量插入的方式,一次性插入insert即可

        List<Row> areaTop3ProductList = areaTop3ProductRDD.collect();
        persistAreaTop3Product(taskid,areaTop3ProductList);

        sc.close();
    }



    /**
     * TODO 查询指定日期范围内的点击行为数据
     * @param sqlContext
     * @param startDate 起始日期
     * @param endDate   截止日期
     * @return  点击行为数据
     */
    private static JavaPairRDD<Long,Row> getCityid2ClickActionRDDByDate(SQLContext sqlContext, String startDate , String endDate){
        //从user_visit_action中,查询用户访问数据,
        //第一个限定:click_profuct_id,限定为不为空的访问行为,那么就代表着点击行为
        //第二个限定:在用户只规定时间范围内的数据
        String sql ="select city_id,click_product_id product_id FROM user_visit_action " +
                "WHERE click_product_id is not NULL " +
             /*   "and click_product_id !='null'" +
                "and click_product_id !='NULL'" +*/
                "and date>='"+startDate+"'" +
                "and date<='"+endDate+"'";
        System.out.println("============================"+sql+"==========================");
        DataFrame clickActionDF = sqlContext.sql(sql);
        JavaRDD<Row> clickActionRDD = clickActionDF.javaRDD();

        JavaPairRDD<Long, Row> cityid2clickActionRDD = clickActionRDD.mapToPair(new PairFunction<Row, Long, Row>() {
            @Override
            public Tuple2<Long, Row> call(Row row) throws Exception {
                Long cityid = row.getLong(0);
                return new Tuple2<Long, Row>(cityid,row);
            }
        });
        return cityid2clickActionRDD;
    }

    /**
     *TODO  使用Spark SQL 从Mysql中查询城市信息
     * @param sqlContext
     * @return
     */
    private static JavaPairRDD<Long,Row> getCityid2CityinfoRDD (SQLContext sqlContext){
        //构建MYSQL连接配置信息<直接从配置文件中获取
        String url =null;
        String user=null;
        String password = null;
        boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
        if (local){
            url = ConfigurationManager.getProperty(Constants.JDBC_URL);
            user=ConfigurationManager.getProperty(Constants.JDBC_USER);
            password=ConfigurationManager.getProperty(Constants.JDBC_PASSWORD);
        }else {
            url = ConfigurationManager.getProperty(Constants.JDBC_URL_SPARK);
            user=ConfigurationManager.getProperty(Constants.JDBC_USER);
            password=ConfigurationManager.getProperty(Constants.JDBC_PASSWORD);
        }

        Map<String,String> options = new HashMap<String, String>();
        options.put("url",url);
        options.put("dbtable","city_info");
        options.put("user",user);
        options.put("password",password);
        //通过SQLContext去从Mysql中查询数据
        DataFrame cityInfoDF = sqlContext.read().format("jdbc").options(options).load();
        JavaRDD<Row> cityInfoRDD = cityInfoDF.javaRDD();
        JavaPairRDD<Long, Row> cityid2cityInfoRDD = cityInfoRDD.mapToPair(new PairFunction<Row, Long, Row>() {
            @Override
            public Tuple2<Long, Row> call(Row row) throws Exception {
                Long cityid = Long.valueOf(String.valueOf(row.get(0)));
                return new Tuple2<Long, Row>(cityid, row);
            }
        });
        return cityid2cityInfoRDD;
    }


    /**
     * TODO 关联点击行为数据与城市信息数据 生成点击商品基础信息临时表
     * @param sqlContext
     * @param cityid2clickActionRDD
     * @param cityid2cityInfoRDD
     */
    private static void generateClickProductBasicTable(SQLContext sqlContext, JavaPairRDD<Long,Row> cityid2clickActionRDD,  JavaPairRDD<Long,Row> cityid2cityInfoRDD){
        //执行join,进行点击行为数据和城市数据关联
        JavaPairRDD<Long, Tuple2<Row, Row>> joinedRDD = cityid2clickActionRDD.join(cityid2cityInfoRDD);

        //将上面的JavaPairRDD,转换为JavaRDD<Row> (才能将RDD转换为DAtaFrame)
        JavaRDD<Row> mappedRDD = joinedRDD.map(new Function<Tuple2<Long, Tuple2<Row, Row>>, Row>(){
            @Override
            public Row call(Tuple2<Long, Tuple2<Row, Row>> tuple) throws Exception {
                long cityid = tuple._1;
                Row clickAction = tuple._2._1;
                Row cityInfo = tuple._2._2;

                //long productid = clickAction.getLong(1);
                long productid = Long.valueOf(String.valueOf(clickAction.get(1)));
                String cityName = cityInfo.getString(1);
                String area = cityInfo.getString(2);

                return RowFactory.create(cityid,cityName,area,productid);
            }
        });
        //基于JavaRDD<Row>格式,就可以将其转换为DATAFrame
        List<StructField> structFields = new ArrayList<StructField>();
        structFields.add(DataTypes.createStructField("city_id",DataTypes.LongType,true));
        structFields.add(DataTypes.createStructField("city_name",DataTypes.StringType,true));
        structFields.add(DataTypes.createStructField("area",DataTypes.StringType,true));
        structFields.add(DataTypes.createStructField("product_id",DataTypes.LongType,true));

        //两个函数
        //UDF:concat2(),将两个字段拼接起来,用指定的分隔符
        //UDAF:group_concat_distinct(),将一个分组中的多个字段值,用逗号拼接起来,同时进行去重

        StructType schema = DataTypes.createStructType(structFields);

        DataFrame df = sqlContext.createDataFrame(mappedRDD,schema);

        //将DATAFrame中的数据,注册成临时表(tmp_click_product_basic)
        df.registerTempTable("tmp_click_product_basic");
    }

    /**
     * TODO 生成各区域各商品点击次数的临时表
     * @param sqlContext
     */
    private static void generateTempAreaProductClickCountTable(SQLContext sqlContext) {
        //按照area和profuct_id两个字段进行分组
        //可以获取到每个area下的每个product_id的城市信息拼接起来的串
        String sql = "select area, " +
                "product_id, " +
                "count(*) click_count," +
                "group_concat_distinct(concat_long_string(city_id,city_name,':')) city_infos " +
                "FROM tmp_click_product_basic " +
                "group by area,product_id";
//        String sql =
//                "SELECT " +
//                        "real_key product_id_area," +
//                        "count(click_count) click_count," +
//                        "group_concat_distinct(city_infos) city_infos " +
//                        "FROM (" +
//                            "SELECT " +
//                            "remove_random_prefix(product_id_area) product_id_area," +
//                            "click_count," +
//                            "city_infos " +
//                            "FROM (" +
//                                "SELECT " +
//                                "product_id_area," +
//                                "count(*) click_count," +
//                                "group_concat_distinct(concat_long_string(cityid,cityname,':')) city_infos " +
//                                "FROM (" +
//                                    "SELECT " +
//                                    "random_frefix(concat_long_string(product_id,area,':'),10) product_id_area," +
//                                    "city_id," +
//                                    "city_nsame " +
//                                    "from tmp_click_product_basic" +
//                                    ") t1" +
//                                "group by product_id_area" +
//                                ") t2"+
//                            ") t3 " +
//                 "group by real_key";


        //使用Spark SQL执行这条sql
      //  System.out.println("============================"+sql+"==========================");
        DataFrame df = sqlContext.sql(sql);

        //再次将查询出来的数据注册成为一个临时表
        //各区域各商品的点击次数(以及额外的城市列表)
        df.registerTempTable("tmp_area_product_click_count");
    }


    /**
     * TODO 生成区域商品点击次数临时表(包含了商品的完整信息)
     * @param sqlContext
     */
    private static void generateTempAreaFullProductClickCountTable(SQLContext sqlContext){
        //将之前得到的各区域各商品点击次数表,product_id
        //去关联商品信息表,product_id,product_name和product_status
        //product_status 要特殊处理0,1,分别代表了自营和第三方商品,放在一个json串中
        //get_json_object()函数,可以从json串中,获取指定的字段的值
        //if()函数,判断,如果product_status是0,那么就是自营的商品;如果是1 ,那就就是第三方商品
        //area,product_id,click_count,city_infos,product_name,product_status

        //为什么要计算出来商品的经营类型
        //你拿到了某个区域top3热门商品,那么其实这个商品是自营的,还是第三方的
        //其实是很重要的

        //TODO 技术点: UDF and  内置if函数使用
        String sql ="select " +
                "tapcc.area," +
                "tapcc.product_id," +
                "tapcc.click_count," +
                "tapcc.city_infos," +
                "pi.product_name, " +
                "if(get_json_object(extend_info,'product_status')='0','自营商品','第三方商品') product_status " +
                "from tmp_area_product_click_count tapcc " +
                "JOIN product_info pi ON tapcc.product_id = pi.product_id";

//        JavaRDD<Row> rdd = sqlContext.sql("select * from product_info").javaRDD();
//        JavaRDD<Row> flattedRDD = rdd.flatMap(new FlatMapFunction<Row, Row>() {
//            @Override
//            public Iterable<Row> call(Row row) throws Exception {
//                List<Row> list = new ArrayList<Row>();
//                for (int i=0 ;i<10;i++){
//                    long productid = row.getLong(0);
//                    String _productid = i+"_"+productid;
//                    Row _row = RowFactory.create(_productid,row.get(1),row.get(2));
//                }
//                return list;
//            }
//        });
//        StructType _schema = DataTypes.createStructType(Arrays.asList(DataTypes.createStructField("product_id",DataTypes.StringType,true),
//                DataTypes.createStructField("product_name",DataTypes.StringType,true),
//                DataTypes.createStructField("product_status",DataTypes.StringType,true)));
//        DataFrame _df = sqlContext.createDataFrame(flattedRDD,_schema);
//        _df.registerTempTable("tmp_product_info");
//        String _sql ="select " +
//                "tapcc.area," +
//                "remove_random_prefix(topicc.product_id) product_id," +
//                "tapcc.click_count," +
//                "tapcc.city_infos," +
//                "pi.product_name" +
//                "if(get_json_object(extent_info,'product_status')=0,'自营商品','第三方商品') product_status" +
//                "from (" +
//                "select " +
//                "area," +
//                "random_prefix(product_id,10) product_id," +
//                "click_count," +
//                "city_infos " +
//                "form tmp_area_product_click_count" +
//                ") tapcc" +
//                "JOIN tmp_product_info pi ON tapcc.product_id = pi.product_id";
       // System.out.println("============================"+sql+"==========================");
        DataFrame df = sqlContext.sql(sql);

        df.registerTempTable("tmp_area_fullprod_click_count");

    }

    /**
     *TODO 获取各区域热门商品
     * @param sqlContext
     */
    private static JavaRDD<Row> generateAreaTop3ProductRDD(SQLContext sqlContext){
        //TODO 技术点: 开窗函数

        //使用开窗函数先进行子查询
        //按照area进行分组,给每个分组内的数据,按照点击次数降序排序,打上一个组内的行号
        //接着在外层查询中,过滤出各个组内的行号排名前3的数据
        //其实就是咱们的各个区域下top3热门商品

        //华北,华东,华南,华中,西北,西南,东北
        //A级:华北,华东
        //B级:华南,华中
        //C级:西北,西南
        //D级:东北

        //case when then .. when then ..else ..end
        String sql=
                "select " +
                        "area," +
                        "CASE " +
                            "WHEN area='华北' OR area='华东' THEN 'A级'" +
                            "WHEN area='华南' OR area='华中' THEN 'B级'" +
                            "WHEN area='西北' OR area='西南' THEN 'C级'" +
                            "ELSE 'D级'" +
                        "END area_level," +
                        "product_id," +
                        "click_count," +
                        "city_infos," +
                        "product_name," +
                        "product_status " +
                  "FROM (" +
                        "select " +
                        "area," +
                        "product_id," +
                        "click_count," +
                        "city_infos," +
                        "product_name," +
                        "product_status," +
                        "row_number() OVER (PARTITION By area ORDER BY click_count DESC) rank " +
                        "from tmp_area_fullprod_click_count " +
                        ") t " +
                        "where rank<=3";
       // System.out.println("============================"+sql+"==========================");
        DataFrame df = sqlContext.sql(sql);
        return df.javaRDD();
    }


    /**
     * 将计算出来的各区域各商品top3热门商品写入mysql中
     * @param rows
     */
    private static void persistAreaTop3Product(Long taskid ,List<Row> rows){
        List<AreaTop3Product> areaTop3Products = new ArrayList<AreaTop3Product>();

        for (Row row : rows){
            AreaTop3Product areaTop3Product = new AreaTop3Product();
            areaTop3Product.setTaskid(taskid);
            areaTop3Product.setArea(row.getString(0));
            areaTop3Product.setAreaLevel(row.getString(1));
            areaTop3Product.setProductid(row.getLong(2));
            areaTop3Product.setClickCount(Long.valueOf(String.valueOf(row.get(3))));
            areaTop3Product.setCityInfos(row.getString(4));
            areaTop3Product.setProductName(row.getString(5));
            areaTop3Product.setProductStatus(row.getString(6));

            areaTop3Products.add(areaTop3Product);
        }

        IAreaTop3ProductDAO areaTop3ProductDAO = DAOFactory.getAreaTop3ProductDAO();
        areaTop3ProductDAO.insertBatch(areaTop3Products);
    }
}
