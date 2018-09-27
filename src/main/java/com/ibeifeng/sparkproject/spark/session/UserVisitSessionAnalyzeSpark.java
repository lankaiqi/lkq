package com.ibeifeng.sparkproject.spark.session;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Optional;
import com.ibeifeng.sparkproject.conf.ConfigurationManager;
import com.ibeifeng.sparkproject.constant.Constants;
import com.ibeifeng.sparkproject.dao.*;
import com.ibeifeng.sparkproject.dao.factory.DAOFactory;
import com.ibeifeng.sparkproject.domain.*;
import com.ibeifeng.sparkproject.test.MockData;
import com.ibeifeng.sparkproject.util.*;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.*;

/**
 * 用户访问session分析 spark作业
 *
 * 接收用户创建的分析任务,用户指定任务如下
 */
public class UserVisitSessionAnalyzeSpark {

    public static void main (String[] args){
        //args = new String[]{"1"};
        //构建spark上下文

        SparkConf conf = new SparkConf().setAppName(Constants.SPARK_APP_NAME_SESSION)
                //.setMaster("local")
                .set("spark.storage.memoryFraction","0.5")
                .set("spark.shuffle.consolidateFiles", "true").set("spark.default.parallelism", "5").set("spark.locality.wait", "10")
                .set("spark.shuffle.file.buffe","64").set("spark.shuffle.memoryFraction","0.3")
                .set("spark.serializer" , "org.apache.spark.serializer.KryoSerializer")
                .set("spark.reducer.maxSizeInFligh" , "24")
                .set("spark.shuffle.io.maxRetrie" , "60").set("spark.shuffle.io.retryWait" , "60")
                .registerKryoClasses(new Class[]{CategorySortKey.class,IntList.class});
        //SparkUtils.setMaster(conf);
        /**
         * TODO 比如,获取top10热门品类功能中,二次排序,自定义一个Key
         * 哪个Key是需要在进行shuffer的时候,进行网络传输的,因此也是需要进行序列化的
         * 启用Kryo机制以后,就会用Kryo的序列化和反序列化CategorySortKey
         * 所以这里要求,为了获取最佳性能,注册一下我们自定义的类
         */
        JavaSparkContext sc = new JavaSparkContext(conf);
        //TODO 设置checkpoint到hdfs的文件目录
        //sc.checkpointFile("hdfs://");
        SQLContext sqlContext = getSQLContext(sc.sc());

        //生成模拟测试数据
        SparkUtils.mockData(sc,sqlContext);
       // mockData(sc,sqlContext);
        //创建需要使用的DAO辅助组件
        ITaskDao taskDao = DAOFactory.getTaskDao();

        //首先查询出来指定的任务,并获取任务查询参数
        long taskid = ParamUtils.getTaskIdFromArgs(args,Constants.SPARK_LOCAL_TASKID_SESSION);
        Task task = taskDao.findById(taskid);//TODO model
        if (task == null ){
            System.out.println(new Date() +":connot find this task with id {"+taskid+"}");
            return;
        }
        JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());//TODO JSONObject

        //如果要进行session聚合,
        //TODO  首先要从user_visit_action表中,查询出来指定日期范围内的行为数据
        /**
         * actionRDD就是一个公共的RDD
         * 第一 要用actionRDD,获取到一个公共的sessiond为key的PairRDD
         * 第二 actionRDD用在了session聚合环节
         *
         * sessionid为key的PairRDD,是确定了,在后面要多次使用的
         * 1 与通过筛选的sessionid进行join,获取通过筛选的session的明细数据
         * 2 将这个RDD直接传入aggregateBySession方法,进行session聚合统计
         *
         * 重构完以后,actionRDD,就只在最开始,使用一次,用来生成以sessionid为key的RDD
         */
        JavaRDD<Row> actionRDD = SparkUtils.getActionRDDByDateRange(sqlContext, taskParam);//TODO Row
        //TODO 获取sessionid2到访问行为数据的映射的RDD  <sessionid,Row>
        JavaPairRDD<String, Row> sessionid2ActionRDD = getSessionid2ActionRDD(actionRDD);
        /**
         * TODO 持久化,很简单,就是对RDD调用persist()方法,并传入一个持久化级别
         *
         * 如果是persist(StorageLevel.MEMORY_ONLY()),纯内存,无序列化,那么就可以用cache()方法来替代
         * StorageLevel.MEMORY_ONLY_SER() 第二选择 内存序列化
         * StorageLevel.MEMORY_AND_DISK() 第三选择
         * StorageLevel.MEMORY_AND_DISK_SER() 第四选择
         * StorageLevel.DISK_ONLY() 第五选择
         *
         * 如果内存充足,要使用双副本高可靠机制
         * 选择后缀带_2的策略
         *StorageLevel.MEMORY_ONLY_2()
         */
        sessionid2ActionRDD = sessionid2ActionRDD.persist(StorageLevel.MEMORY_ONLY());
        //TODO checkpoint 将RDD存储到hdfs中
        //sessionid2ActionRDD.checkpoint();
        //首先,可以将行为数据session_id进行groupByKey分组
        //此时数据粒度就是session粒度了,然后,可以将session粒度数据与用户信息数据,进行join
        //然后就可以获取到session粒度数据,同时,数据里面还包含了session对应的user的信息
        //TODO  到这里为止，获取的数据是<sessionid,(sessionid,searchKeywords,clickCategoryIds,age,professional,city,sex)>
        JavaPairRDD<String, String> sessionid2AggrInfoRDD = aggregateSession(sc,sqlContext, sessionid2ActionRDD); //TODO sessionid2AggrInfoRDD:session粒度聚合数据   <session_id,row> row=点击,搜索,城市,工作,性别,年龄等集合
       /* System.out.println("============================"+sessionid2AggrInfoRDD.count()+"==========================");
        for ( Tuple2<String,String> tuple:sessionid2AggrInfoRDD.take(10)){
            System.out.println("============================"+tuple._2+"==========================");
        }*/
        //接着,针对session粒度的聚合数据,按照使用者指定的筛选参数进行数据过滤
        //相当于我们自己编写的算子,是要访问外面的任务参数对象的
        //所以,大家要记得我们之前说的,匿名内部类(算子函数),访问外部对象,是要给外部对象使用final修饰的

        //TODO 重构,同时进行过滤,统计
        Accumulator<String> sessionAggrStatAccumulator = sc.accumulator("", new SessionAggrStatAccumulator());
        //TODO 过滤session数据,并进行统计  <session,(时长,步长等)>
        JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD  = filterSessionAndAggrStat(sessionid2AggrInfoRDD, taskParam,sessionAggrStatAccumulator);
        filteredSessionid2AggrInfoRDD = filteredSessionid2AggrInfoRDD.persist(StorageLevel.MEMORY_ONLY());
        /**
         * 重构 sessionid2detailRDD,就是代表了通过筛选的session对应的访问明细数据
         */
        //TODO 生成公共的RDD,通过筛选条件的session的访问明细数据
        JavaPairRDD<String, Row> sessionid2detailRDD = getSessionid2detailRDD(filteredSessionid2AggrInfoRDD, sessionid2ActionRDD);
        sessionid2detailRDD = sessionid2detailRDD.persist(StorageLevel.MEMORY_ONLY());
        /**
         * 对于Accumulator这种分布式累加计算变量的使用,有一个重要的说明
         *
         * 从Accumulator中,获取数据,插入数据库的时候,一定要,一定要,是在某个action操作后再进行...
         *
         * 如果没有action的话,那么整个程序根本不会运行
         *
         * 必须把能够触发job执行的操作,必须放在mysql执行之前
         */
        // System.out.println("============================"+filteredSessionid2AggrInfoRDD.count()+"==========================");
        // TODO 随机抽取session
        randomExtractSession(sc,filteredSessionid2AggrInfoRDD,task.getTaskid(),sessionid2ActionRDD);
        /**
         * 特别说明
         * 我们知道,要将上一个功能的session聚合统计数据获取到,就必须是在一个action操作触发job之后
         * 才能从Accumulator中获取数据,否则是获取不到数据的,因为没有job执行,Accumulator的值为空
         * 所以,我们在这里,将随机抽取的功能的实现代码,放在session聚合统计功能的最终计算和写库之前
         * 因为随机抽取功能中,有一个countByKey算子,是action操作,会触发job
         */
        //TODO 计算出各个范围的session占比,并写入mysql
        calulateAndPersistAggrSet(sessionAggrStatAccumulator.value(),task.getTaskid());
        /**
         *  session聚合统计,(统计出访问时长和访问步长,各个区间的session数量占总session数量的比例)
         *
         *  如果不进行重构,直接来实现,思路:
         *  1 actionRDD,映射<sessionid,Row>的格式
         *  2 按sessionid聚合,每个session的访问时长,访问步长
         *  3 遍历新生成的RDD,将每个session的访问时长和访问步长,去更新自定义Accumulator中对应的值
         *  4 使用自定义Accumutor中的统计值,去计算各个区间的比例
         *  5 将最后计算出来的结果,写入mysql对应的表中
         *
         *  普通实现的问题:
         *  1 为什么还要用actionRDD去映射? 其实我们职权session集合,映射已经做过,多此一举
         *  2 是不是一定要为了session聚合功能,单独去遍历一遍session?其实没有必要,已经有session数据
         *    之前过滤session的时候,相当于,是在遍历session,没必要再过滤一遍
         *
         *  重构实现思路:
         *  1 不要生成新的RDD(处理上亿数据)
         *  2 不要单独遍历session的数据(处理上千万数据)
         *  3 可以再进行session聚合的时候,就直接计算出来每个session的访问时长,访问步长
         *  4 在进行过滤的时候本来就遍历所有的session聚合信息,此时,就可以在某个session通过筛选条件后,将其访问时长访问步长,累加到自定义的Accumulator上面
         *  5 就是两种不同的方式,在面对上亿千万数据,可以节省半小时甚至数个小时
         *
         *  开发Spark项目经验:
         *  1 尽量少生成RDD
         *  2 尽量少对RDD进行算子操作,如果可能,尽量在一个算子里面,实现多个需要做的功能
         *  3 尽量少对RDD进行shuffer操作,如:groupByKey,reduceByKey,sortByKey,,,shuffer操作会大量读写磁盘,严重降低性能
         *      有shuffer算子,跟没有的算子,可能会有数十数小时差别
         *      有shuffer算子,可能会导致数据倾斜
         *  4 无论做什么功能.性能第一
         *
         */
        //TODO 获取top10热门品类  <CategorySortKey , <商品id,点击数,下单数,支付数.>
        List<Tuple2<CategorySortKey, String>> top10Category = getTop10Category(task.getTaskid(), sessionid2detailRDD);

        //TODO 获取top10活跃session
        getTop10Session(sc,task.getTaskid(),top10Category,sessionid2detailRDD);

        //关闭spark上下文
        sc.close();
    }


    /**
     * TODO 获取SQLContext
     * TODO 本地测试生成sqlcontext,生产环境生成HIVEContext对象
     * @param sc
     * @return
     */
    private static SQLContext getSQLContext (SparkContext sc){
        boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
        if (local){
            return new SQLContext(sc);
        }else {
            return new HiveContext(sc);
        }
    }

    /**
     * TODO 生成模拟数据,(只有本地模式生成模拟数据)
     * @param sc
     * @param sqlContext
     */
    private static void mockData( JavaSparkContext sc,SQLContext sqlContext){
        boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
        if (local){
            MockData.mock(sc,sqlContext);
        }
    }

    /**
     * TODO 获取指定日期范围内的用户访问行为数据
     * @param sqlcontext
     * @param taskParam 任务参数
     * @return 行为数据RDD
     */
    private static JavaRDD<Row> getActionRDDByDateRange(SQLContext sqlcontext , JSONObject taskParam){
        String startDate = ParamUtils.getParam(taskParam,Constants.PARAM_START_DATE);
        String endDate = ParamUtils.getParam(taskParam,Constants.PARAM_END_DATE);
        String sql ="select * from user_visit_action where date>= '"+startDate+"' and date <= '"+endDate+"'";
        DataFrame actionDF = sqlcontext.sql(sql);
        /**
         * 这里就很有可能发生partiton数量过少的问题
         * 比如,sqparkSQL默认给第一个stage设置了20个task,但是根据你的数据量以及算法的复杂度,
         * 实际上需要1000个task
         *
         * 所以,这里,就可以对Spark SQL查出的RDD执行repartition重分区操作
         */
        //return actionDF.javaRDD().repartition(1000);
        return actionDF.javaRDD();
    }

    /**
     * 获取sessionid2到访问行为数据的映射的RDD
     * @param actionRDD
     * @return
     */
    private static JavaPairRDD<String,Row> getSessionid2ActionRDD(JavaRDD<Row> actionRDD){
//        return actionRDD.mapToPair(new PairFunction<Row, String, Row>(){
//            @Override
//            public Tuple2<String, Row> call(Row row) throws Exception {
//                return new Tuple2<String, Row>(row.getString(2),row);
//            }
//        });

        return actionRDD.mapPartitionsToPair(new PairFlatMapFunction<Iterator<Row>, String, Row>() {
            @Override
            public Iterable<Tuple2<String, Row>> call(Iterator<Row> iterator) throws Exception {
                List<Tuple2<String,Row>> list = new ArrayList<Tuple2<String, Row>>();
                while (iterator.hasNext()){
                    Row row = iterator.next();
                    list.add(new Tuple2<String, Row>(row.getString(2),row));
                }
                return  list;
            }
        });
    }
    /**
     * TODO 对行为数据按session粒度进行聚合
     * @param //actionRDD 行为数据RDD
     * @return  session粒度聚合数据   <session_id,row> row=点击,搜索,城市,工作,性别,年龄等集合
     */
    private static JavaPairRDD<String,String> aggregateSession(JavaSparkContext sc,SQLContext sqlContext,  JavaPairRDD<String, Row> sessionid2ActionRDD){
        //现在actionRDD中的元素是ROW,一个Row就是一行用户行为数据,
        // 我们现在需要将这个Row映射成<sessionid,Row>的格式
/*        JavaPairRDD<String,Row> sessionid2ActionRDD = actionRDD.mapToPair(
                //PairFunction
                //第一个参数相当于函数输入,
                //第二个三个相当于,函数输出(Tuple),分别是Tuple第一第二个值
                new PairFunction<Row, String, Row> (){
                    @Override
                    public Tuple2<String, Row> call(Row row) throws Exception {
                        return new Tuple2<String, Row>(row.getString(2),row);
                    }
                });*/
        //对行为数据按session粒度进行分组
        JavaPairRDD<String, Iterable<Row>> session2ActionsRDD = sessionid2ActionRDD.groupByKey();

        //对每一个session分组进行聚合,将session中所有的搜索词和点击品类都聚合起来
        //TODO 到此为止，获取的数据格式，如下：<userid,partAggrInfo(sessionid,searchKeywords,clickCategoryIds)>
        JavaPairRDD<Long, String> userid2PartAggrInfoRDD = session2ActionsRDD.mapToPair(new PairFunction<Tuple2<String, Iterable<Row>>, Long, String>() {
            @Override
            public Tuple2<Long, String> call(Tuple2<String, Iterable<Row>> tuple) throws Exception {
                String sessionid = tuple._1;
                Iterator<Row> iterator = tuple._2.iterator();

                StringBuffer sessionKeywordsBuffer = new StringBuffer("");
                StringBuffer clickcategoryIdsBuffer = new StringBuffer("");

                Long userid = null;

                //session的起始结束时间
                Date startTIme = null;
                Date endTime = null;
                //session的访问步长
                int strpLength = 0;


                //遍历session所有的访问行为
                while(iterator.hasNext()){
                    System.out.println("============================"+"111111111111`"+"==========================");
                    //提取每个访问行为的搜索词字段和点击品类字段
                    Row row = iterator.next();
                    if (userid == null){
                        userid = row.getLong(1);
                    }
                    String searchKeyword = row.getString(5);//search_keyword
                    System.out.println("===============searchKeyword============="+searchKeyword+"==========================");
                    Long clickCategoryId = row.getLong(6);//click_category_id
                    //  System.out.println("===========searchKeyword================="+searchKeyword+"==========================");
                     // System.out.println("===============clickCategoryId============="+clickCategoryId+"==========================");
                    //实际上要对数据说明一下
                    //并不是每一行访问行为这两个字段都有,
                    //只有搜索行为有search_keyword字段
                    //只有点击行为有click_category_id字段
                    //所以,任何行为,都只有一种数据,所以能出想null
                    //我们要将搜索,点击行为id拼到字符串中,
                    //首先,不能是null,其次,之前的字符串还没有搜索词或者点击品类id

                    if(StringUtils.isNotEmpty(searchKeyword)){
                        if(!sessionKeywordsBuffer.toString().contains(searchKeyword)){
                            sessionKeywordsBuffer.append(searchKeyword+",");
                        }
                    }
                    //System.out.println("===============sessionKeywordsBuffer============="+sessionKeywordsBuffer+"==========================");
                    if(clickCategoryId != null ){
                        if(!clickcategoryIdsBuffer.toString().contains(String.valueOf(clickCategoryId))){
                            clickcategoryIdsBuffer.append(clickCategoryId+",");
                        }
                    }
                    // System.out.println("=================sessionKeywordsBuffer==========="+sessionKeywordsBuffer+"==========================");
                    //计算session开始和结束时间
                    Date actionTime = DateUtils.parseTime(row.getString(4));
                    if(startTIme ==null){
                        startTIme = actionTime;
                    }
                    if(endTime ==null){
                        endTime = actionTime;
                    }
                    if (actionTime.before(startTIme)){
                        startTIme = startTIme;
                    }
                    if(actionTime.after(endTime)){
                        endTime = actionTime;
                    }

                    //计算session访问步长
                    strpLength++;
                }
                String searchKeywords = StringUtils.trimComma(sessionKeywordsBuffer.toString());
                // System.out.println("=======searchKeywords====================="+searchKeywords+"==========================");
                String clickcategoryIds = StringUtils.trimComma(clickcategoryIdsBuffer.toString());
                // System.out.println("==========clickcategoryIds=================="+clickcategoryIds+"==========================");

                //计算session访问时长(秒)
                Long visitLength = (endTime.getTime()-startTIme.getTime())/1000;

                //用key=value拼接
                String partAggrInfo = Constants.FIELD_SESSION_ID+"="+sessionid+"|"
                        +Constants.FIELD_SEARCH_KEYWORDS+"="+searchKeywords+"|"
                        +Constants.FIELD_CLICK_CATEGORY_IDS+"="+clickcategoryIds+"|"
                        +Constants.FIELD_VISIT_LENGTH+"="+visitLength+"|"
                        +Constants.FIELD_STEP_LENGTH+"="+strpLength +"|"
                        +Constants.FIELD_START_TIME+"="+DateUtils.formatTime(startTIme) ;

                return new Tuple2<Long, String>(userid,partAggrInfo);
            }
        });
        //查询所有用户数据 ,并生成<userid,row>格式
        String sql = "select * from user_info";
        JavaRDD<Row> userInfoRDD = sqlContext.sql(sql).javaRDD();
        JavaPairRDD<Long, Row> userid2InfoRdd = userInfoRDD.mapToPair(new PairFunction<Row, Long, Row>() {
            @Override
            public Tuple2<Long, Row> call(Row row) throws Exception {
                return new Tuple2<Long, Row>(row.getLong(0),row);
            }
        });

        /**
         * 这里就比较适合采用reduce join转换为map join的方式
         *
         * userid2PartAggrInfoRDD可能数据量比较大(1千万)
         * userid2InfoRdd可能数据量比较小(10万)
         */
        //将session粒度聚合信息,与用户信息进行join
        JavaPairRDD<Long, Tuple2<String, Row>> userid2FullInfoRdd = userid2PartAggrInfoRDD.join(userid2InfoRdd);
        //对join起来的数据进行拼接,并返回<sessionid,fullAggrInfo>格式数据
        JavaPairRDD<String, String> sessionid2FullAggrInfoRDD = userid2FullInfoRdd.mapToPair(new PairFunction<Tuple2<Long, Tuple2<String, Row>>, String, String>() {
            @Override
            public Tuple2<String, String> call(Tuple2<Long, Tuple2<String, Row>> tuple) throws Exception {
                String partAggrInfo = tuple._2._1;
                Row userInfoRow = tuple._2._2;

                String sessionid = StringUtils.getFieldFromConcatString(partAggrInfo,"\\|",Constants.FIELD_SESSION_ID);

                int age = userInfoRow.getInt(3);
                String professional = userInfoRow.getString(4);
                String city = userInfoRow.getString(5);
                String sex = userInfoRow.getString(6);

                String fullAggInfo = partAggrInfo+"|"
                        +Constants.FIELD_AGE+"="+age+"|"
                        +Constants.FIELD_PROFESSIONAL+"="+professional+"|"
                        +Constants.FIELD_CITY+"="+city+"|"
                        +Constants.FIELD_SEX+"="+sex;
                // System.out.println("===1111========================="+fullAggInfo+"==========================");
                return new Tuple2<String, String>(sessionid,fullAggInfo);
            }
        });

        /**
         * TODO reduce join 转换 map join
         */
//        List<Tuple2<Long, Row>> userInfos = userid2InfoRdd.collect();
//        final Broadcast<List<Tuple2<Long, Row>>> userInfosBroadcast = sc.broadcast(userInfos);
//        final JavaPairRDD<String, String> tuneRDD = userid2PartAggrInfoRDD.mapToPair(new PairFunction<Tuple2<Long, String>, String, String>() {
//            public Tuple2<String, String> call(Tuple2<Long, String> tuple) throws Exception {
//                //得到用户信息的map
//                List<Tuple2<Long,Row>> userInfos = userInfosBroadcast.value();
//
//                Map<Long,Row> userInfoMap = new HashMap<Long, Row>();
//                for(Tuple2<Long,Row> userInfo : userInfos){
//                    userInfoMap.put(userInfo._1,userInfo._2);
//                }
//
//                //获取到当前用户对应的信息
//                String partAggrInfo = tuple._2;
//                Row userInfoRow = userInfoMap.get(tuple._1);
//
//                String sessionid = StringUtils.getFieldFromConcatString(partAggrInfo,"\\|",Constants.FIELD_SESSION_ID);
//
//                int age = userInfoRow.getInt(3);
//                String professional = userInfoRow.getString(4);
//                String city = userInfoRow.getString(5);
//                String sex = userInfoRow.getString(6);
//
//                String fullAggInfo = partAggrInfo+"|"
//                        +Constants.FIELD_AGE+"="+age+"|"
//                        +Constants.FIELD_PROFESSIONAL+"="+professional+"|"
//                        +Constants.FIELD_CITY+"="+city+"|"
//                        +Constants.FIELD_SEX+"="+sex;
//                // System.out.println("===1111========================="+fullAggInfo+"==========================");
//                return new Tuple2<String, String>(sessionid,fullAggInfo);
//            }
//        });
//
//        /**
//         * TODO sample采样,倾斜key单独进行join
//         */
//        JavaPairRDD<Long, String> sampledRDD = userid2PartAggrInfoRDD.sample(false, 0.1, 9);
//        JavaPairRDD<Long, Long> mappedSampledRDD =  sampledRDD.mapToPair(new PairFunction<Tuple2<Long, String>, Long, Long>(){
//            public Tuple2<Long, Long> call(Tuple2<Long, String> tuple) throws Exception {
//                return new Tuple2<Long, Long>(tuple._1,1L);
//            }
//        });
//        JavaPairRDD<Long, Long> computedSampledRDD = mappedSampledRDD.reduceByKey(new Function2<Long, Long, Long>() {
//            public Long call(Long v1, Long v2) throws Exception {
//                return v1 + v2;
//            }
//        });
//        JavaPairRDD<Long, Long> reversedSampleRDD = computedSampledRDD.mapToPair(new PairFunction<Tuple2<Long, Long>, Long, Long>() {
//            public Tuple2<Long, Long> call(Tuple2<Long, Long> tuple) throws Exception {
//                return new Tuple2<Long, Long>(tuple._2, tuple._1);
//            }
//        });
//        final Long skewedUserid = reversedSampleRDD.sortByKey(false).take(1).get(0)._2;
//
//        JavaPairRDD<Long, String> skewedRDD = userid2PartAggrInfoRDD.filter(new Function<Tuple2<Long, String>, Boolean>() {
//            public Boolean call(Tuple2<Long, String> tuple) throws Exception {
//                return tuple._1.equals(skewedUserid);
//            }
//        });
//        JavaPairRDD<Long, String> commonRDD = userid2PartAggrInfoRDD.filter(new Function<Tuple2<Long, String>, Boolean>() {
//            public Boolean call(Tuple2<Long, String> tuple) throws Exception {
//                return !tuple._1.equals(skewedUserid);
//            }
//        });
//        JavaPairRDD<String, Row> skewedUserid2InfoRDD = userid2InfoRdd.filter(new Function<Tuple2<Long, Row>, Boolean>() {
//            public Boolean call(Tuple2<Long, Row> tuple) throws Exception {
//                return tuple._1.equals(skewedUserid);       //key=skewedUserid  的记录(一条)
//            }
//        }).flatMapToPair(new PairFlatMapFunction<Tuple2<Long, Row>, String, Row>(){
//            public Iterable<Tuple2<String, Row>> call(Tuple2<Long, Row> tuple) throws Exception {
//                Random random = new Random();
//
//                List<Tuple2<String,Row>> list = new ArrayList<Tuple2<String, Row>>();
//
//                for (int i=0; i<100 ; i++){
//                    int prefix = random.nextInt(100);
//                    list.add(new Tuple2<String, Row>(prefix+"_"+tuple._1,tuple._2));
//                }
//                return list;    //把一条记录前面加上前缀,生成100条数据
//            }
//        });
//
//        //JavaPairRDD<Long, Tuple2<String, Row>> joinedRDD1 = skewedRDD.join(userid2InfoRdd);
//        JavaPairRDD<Long, Tuple2<String,Row>> joinedRDD1 = skewedRDD.mapToPair(new PairFunction<Tuple2<Long, String>, String,String>(){
//            public Tuple2<String,String> call(Tuple2<Long, String> tuple) throws Exception {
//                Random random = new Random();
//                int prefix = random.nextInt(100);
//                return new Tuple2<String, String>(prefix+"_"+tuple._1,tuple._2);
//            }
//        }).join(skewedUserid2InfoRDD)//<String,<row,row>>
//                .mapToPair(new PairFunction<Tuple2<String, Tuple2<String, Row>>, Long, Tuple2<String,Row>>(){
//                    public Tuple2<Long, Tuple2<String,Row>> call(Tuple2<String, Tuple2<String, Row>> tuple) throws Exception {
//                        long userid = Long.valueOf(tuple._1.split("_")[1]);
//                        return new Tuple2<Long, Tuple2<String,Row>>(userid,tuple._2);
//                    }
//                });
//
//        JavaPairRDD<Long, Tuple2<String, Row>> joinedRDD2 = commonRDD.join(userid2InfoRdd);
//        JavaPairRDD<Long, Tuple2<String, Row>> joinedRDD = joinedRDD1.union(joinedRDD2);
//
//        JavaPairRDD<String, String> finalRDD = joinedRDD.mapToPair(new PairFunction<Tuple2<Long, Tuple2<String, Row>>, String, String>() {
//            @Override
//            public Tuple2<String, String> call(Tuple2<Long, Tuple2<String, Row>> tuple) throws Exception {
//                String partAggrInfo = tuple._2._1;
//                Row userInfoRow = tuple._2._2;
//
//                String sessionid = StringUtils.getFieldFromConcatString(partAggrInfo,"\\|",Constants.FIELD_SESSION_ID);
//
//                int age = userInfoRow.getInt(3);
//                String professional = userInfoRow.getString(4);
//                String city = userInfoRow.getString(5);
//                String sex = userInfoRow.getString(6);
//
//                String fullAggInfo = partAggrInfo+"|"
//                        +Constants.FIELD_AGE+"="+age+"|"
//                        +Constants.FIELD_PROFESSIONAL+"="+professional+"|"
//                        +Constants.FIELD_CITY+"="+city+"|"
//                        +Constants.FIELD_SEX+"="+sex;
//                // System.out.println("===1111========================="+fullAggInfo+"==========================");
//                return new Tuple2<String, String>(sessionid,fullAggInfo);
//            }
//        });


        /**
         * TODO 使用随机数和扩容表进行join
         */
//        JavaPairRDD<String, Row> expandedRDD = userid2InfoRdd.flatMapToPair(new PairFlatMapFunction<Tuple2<Long, Row>, String, Row>() {
//            public Iterable<Tuple2<String, Row>> call(Tuple2<Long, Row> tuple) throws Exception {
//                List<Tuple2<String , Row>> list = new ArrayList<Tuple2<String, Row>>();
//
//                for (int i=0 ; i<10 ; i++){
//                    list.add(new Tuple2<String, Row>(0+"_"+tuple._1,tuple._2));
//                }
//                return list;
//            }
//        });
//
//        JavaPairRDD<String, String> mappedRDD = userid2PartAggrInfoRDD.mapToPair(new PairFunction<Tuple2<Long, String>, String, String>() {
//            @Override
//            public Tuple2<String, String> call(Tuple2<Long, String> tuple) throws Exception {
//                Random random = new Random();
//                int prefix = random.nextInt(10);
//                return new Tuple2<String, String>(prefix + "_" + tuple._1, tuple._2);
//            }
//        });
//        JavaPairRDD<String, Tuple2<String, Row>> joinedRDD = mappedRDD.join(expandedRDD);
//        JavaPairRDD<String, String> finalRDD = joinedRDD.mapToPair(new PairFunction<Tuple2<String, Tuple2<String, Row>>, String, String>() {
//            public Tuple2<String, String> call(Tuple2<String, Tuple2<String, Row>> tuple) throws Exception {
//                String partAggrInfo = tuple._2._1;
//                Row userInfoRow = tuple._2._2;
//
//                String sessionid = StringUtils.getFieldFromConcatString(partAggrInfo,"\\|",Constants.FIELD_SESSION_ID);
//
//                int age = userInfoRow.getInt(3);
//                String professional = userInfoRow.getString(4);
//                String city = userInfoRow.getString(5);
//                String sex = userInfoRow.getString(6);
//
//                String fullAggInfo = partAggrInfo+"|"
//                        +Constants.FIELD_AGE+"="+age+"|"
//                        +Constants.FIELD_PROFESSIONAL+"="+professional+"|"
//                        +Constants.FIELD_CITY+"="+city+"|"
//                        +Constants.FIELD_SEX+"="+sex;
//                // System.out.println("===1111========================="+fullAggInfo+"==========================");
//                return new Tuple2<String, String>(sessionid,fullAggInfo);
//            }
//        });


        return sessionid2FullAggrInfoRDD;
    }

    /**
     * TODO 过滤session数据,并进行统计
     * @param sessionid2AggrInfoRDD
     * @return
     */
    private static JavaPairRDD<String, String> filterSessionAndAggrStat(JavaPairRDD<String, String> sessionid2AggrInfoRDD, final JSONObject taskParam, final Accumulator<String> sessionAggrStatAccumulator){
        //为了使用后面的ValueUtils,所以,首先将所有的筛选参数拼接成一个连接串
        //此外,这里是给我们后面的性能优化买下了一个伏笔
        String startAge =  ParamUtils.getParam(taskParam,Constants.PARAM_START_AGE);
        String endAge =  ParamUtils.getParam(taskParam,Constants.PARAM_END_AGE);
        String professionals =  ParamUtils.getParam(taskParam,Constants.PARAM_PROFESSIONALS);
        String cities =  ParamUtils.getParam(taskParam,Constants.PARAM_CITIES);
        String sex =  ParamUtils.getParam(taskParam,Constants.PARAM_SEX);
        String keywords =  ParamUtils.getParam(taskParam,Constants.PARAM_KEYWORDS);
        String categoryIds =  ParamUtils.getParam(taskParam,Constants.PARAM_CATEGOTY_IDS);

        String _parameter = (startAge!=null?Constants.PARAM_START_AGE+"="+startAge +"|": "" )
                +(endAge!=null?Constants.PARAM_END_AGE+"="+endAge  +"|": "" )
                +(professionals!=null?Constants.PARAM_PROFESSIONALS+"="+professionals  +"|": "" )
                +(cities!=null?Constants.PARAM_CITIES+"="+cities  +"|": "" )
                +(sex!=null?Constants.PARAM_SEX+"="+sex  +"|": "" )
                +(keywords!=null?Constants.PARAM_KEYWORDS+"="+keywords  +"|": "" )
                +(categoryIds!=null?Constants.PARAM_CATEGOTY_IDS+"="+categoryIds : "" );
        if (_parameter.endsWith("\\|")){
            _parameter =_parameter.substring(0,_parameter.length()-1);
        }
        final String parameter=_parameter;
        //根据筛选参数进行过滤
        JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD = sessionid2AggrInfoRDD.filter(new Function<Tuple2<String, String>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, String> tuple) throws Exception {
                //首先,从tuple中获取聚合数据
                String aggrInfo = tuple._2;
                // System.out.println("===1111========================="+aggrInfo+"==========================");
                //接着,依次按照条件过滤
                //按照年龄范围过滤(startAge,endAge)
                if (!ValidUtils.between(aggrInfo,Constants.FIELD_AGE,parameter,Constants.PARAM_START_AGE,Constants.PARAM_END_AGE)){
                    return false;
                }
                //按照职业范围进行分割
                if (!ValidUtils.in(aggrInfo,Constants.FIELD_PROFESSIONAL,parameter,Constants.PARAM_PROFESSIONALS)){
                    return false;
                }
                //按照城市进行过滤
                if (!ValidUtils.in(aggrInfo,Constants.FIELD_CITY,parameter,Constants.PARAM_CITIES)){
                    return false;
                }
                //按照性别过滤
                if (!ValidUtils.equal(aggrInfo,Constants.FIELD_SEX,parameter,Constants.PARAM_SEX)){
                    return false;
                }
                //按照搜索词过滤
                if (!ValidUtils.in(aggrInfo,Constants.FIELD_SEARCH_KEYWORDS,parameter,Constants.PARAM_KEYWORDS)){
                    return false;
                }
                //按照点击品类id进行过滤
                if (!ValidUtils.in(aggrInfo,Constants.FIELD_CLICK_CATEGORY_IDS,parameter,Constants.PARAM_CATEGOTY_IDS)){
                    return false;
                }

                //如果经过之前多个过滤条件,程序走到这里
                //说明,该session是需要保留的
                //那么就要对session的访问时长,访问步长进行统计,根据session对应的范围,
                //进行相应的累加计数

                //主要走到这一步,那么就是需要计数的session
                sessionAggrStatAccumulator.add(Constants.SESSION_COUNT);

                //根据session的访问时长与步长的范围,进行相应的累加
                long visitLength = Long.valueOf(StringUtils.getFieldFromConcatString(aggrInfo,"\\|",Constants.FIELD_VISIT_LENGTH));
                long stepLength = Long.valueOf(StringUtils.getFieldFromConcatString(aggrInfo,"\\|",Constants.FIELD_STEP_LENGTH));
                calulateVisitLength(visitLength);
                calculateStepLength(stepLength);
                return true;
            }

            /**
             * 计算访问时长范围
             * @param visitLength
             */
            private void calulateVisitLength (long visitLength){
                // System.out.println("============================"+visitLength+"==========================");
                if (visitLength >=0 && visitLength<=3){
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1s_3s);
                }else if(visitLength >=4 && visitLength <= 6) {
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_4s_6s);
                } else if(visitLength >=7 && visitLength <= 9) {
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_7s_9s);
                } else if(visitLength >=10 && visitLength <= 30) {
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10s_30s);
                } else if(visitLength > 30 && visitLength <= 60) {
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30s_60s);
                } else if(visitLength > 60 && visitLength <= 180) {
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1m_3m);
                } else if(visitLength > 180 && visitLength <= 600) {
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_3m_10m);
                } else if(visitLength > 600 && visitLength <= 1800) {
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10m_30m);
                } else if(visitLength > 1800) {
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30m);
                }
            }

            /**
             * 计算访问步长范围
             * @param stepLength
             */
            private void calculateStepLength(long stepLength) {
                if(stepLength >= 1 && stepLength <= 3) {
                    sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_1_3);
                } else if(stepLength >= 4 && stepLength <= 6) {
                    sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_4_6);
                } else if(stepLength >= 7 && stepLength <= 9) {
                    sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_7_9);
                } else if(stepLength >= 10 && stepLength <= 30) {
                    sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_10_30);
                } else if(stepLength > 30 && stepLength <= 60) {
                    sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_30_60);
                } else if(stepLength > 60) {
                    sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_60);
                }
            }
        });
        return filteredSessionid2AggrInfoRDD;
    }

    /**
     * TODO 获取通过筛选条件的session的访问明细数据RDD
     * @param sessionid2aggrInfoRDD             过滤100个sesison聚合 |
     * @param sessionid2ActionRDD               全部session的原始的值
     * @return
     */
    private static JavaPairRDD<String,Row> getSessionid2detailRDD(JavaPairRDD<String,String> sessionid2aggrInfoRDD, JavaPairRDD<String,Row> sessionid2ActionRDD){
        JavaPairRDD<String,Row> sessionid2detailRDD = sessionid2aggrInfoRDD.join(sessionid2ActionRDD).mapToPair(new PairFunction<Tuple2<String, Tuple2<String, Row>>, String, Row>(){
            @Override
            public Tuple2<String, Row> call(Tuple2<String, Tuple2<String, Row>> tuple) throws Exception {
                return new Tuple2<String, Row>(tuple._1,tuple._2._2);
            }
        });
        return sessionid2detailRDD;
    }
    /**
     * TODO 随机抽取session
     * @param sessionid2AggrInfoRDD
     */
    private static void randomExtractSession(JavaSparkContext sc,JavaPairRDD<String ,String> sessionid2AggrInfoRDD, final long taskid,JavaPairRDD<String,Row> sessionid2ActionRDD){
        //第一步,计算出每天每小时的session数量,获取<yyyy-MM-dd_HH,aggrInfo>格式的RDD
        JavaPairRDD<String,String> time2sessionRDD = sessionid2AggrInfoRDD.mapToPair(new PairFunction<Tuple2<String, String>, String, String>(){
            @Override
            public Tuple2<String, String> call(Tuple2<String, String> tuple) throws Exception {

                String sessionid = tuple._1;
                String aggrInfo = tuple._2;

                String startTime = StringUtils.getFieldFromConcatString(aggrInfo,"\\|",Constants.FIELD_START_TIME);

                String dateHour = DateUtils.getDateHour(startTime);
                //  System.out.println("============================"+aggrInfo+"==========================");
                return new Tuple2<String, String>(dateHour,aggrInfo);
            }
        });

        /**
         * 思考,不要着急写大量代码,做项目,多思考
         *
         * 每天每小时的sesssion数量,每天每小时的session抽取索引,遍历每天每小时session
         * 首先抽取出session的聚合数据,写入session_random_extract表
         * 所以第一个RDD的value,应该是session聚合数据
         */

        //得到每天每小时的session数量
        /**
         * 每天每小时session数量计算,可能出现数据倾斜
         * 大部分时间10w访问量,中午高峰期,1h有1000W数据量
         * 这种就有数据倾斜
         *
         * 用countByKey操作,给演示第四种方案
         */
        Map<String, Object> countMap = time2sessionRDD.countByKey();

        //第二部,使用按时间比例随机抽取算法,计算出每天每小时要抽取的session索引
        HashMap<String, Map<String, Long>> dayHourCountMap = new HashMap<String, Map<String, Long>>();
        //将<yyyy-MM-dd_HH,count>格式的map,装换成<yyyy-MM-dd,<HH,count>>的格式
        for ( Map.Entry<String,Object> countEntry: countMap.entrySet()){
            String dateHour = countEntry.getKey();
            String date =dateHour.split("_")[0];
            String hour =dateHour.split("_")[1];

            Long count = Long.valueOf(String.valueOf(countEntry.getValue()));

            Map<String,Long> hourCountMap = dayHourCountMap.get(date);
            if (hourCountMap == null) {
                hourCountMap = new HashMap<String, Long>();
                dayHourCountMap.put(date, hourCountMap);
            }
            hourCountMap.put(hour,count);
        }
        //按时间比例随机抽取算法
        //总共要抽取100个session,按照天进行平分
        // System.out.println("============================"+dayHourCountMap.size()+"==========================");
        int extracNumberPerDay = 100/ dayHourCountMap.size();


        /**
         * session随机抽取功能
         * 用到了一个比较大的变量,随机抽取索引map
         * 之前是直接在算子里使用这个map,那么根据我们刚才讲的这个原理,每个task都会拷贝一份map副本
         * 还是比较消耗内存和网络传输性能的
         *
         * 将map做成广播变量
         *
         */
        //<date,<hour,(3,5,7,78,109)>>
        Map<String,Map<String,List<Integer>>>  dateHourExtractMap = new HashMap<String,Map<String,List<Integer>>>();
        Random random = new Random();

        for(Map.Entry<String , Map<String,Long>> dateHourCountEntry: dayHourCountMap.entrySet()){
            String date = dateHourCountEntry.getKey();
            Map<String, Long> hourCountMap = dateHourCountEntry.getValue();

            //计算出这一天的session总数
            long sessionCount = 0L;
            for(long hourCount:hourCountMap.values()){
                sessionCount += hourCount;
            }

            Map<String,List<Integer>> hourExtractMap = dateHourExtractMap.get(date);
            if (hourExtractMap == null){
                hourExtractMap = new HashMap<String, List<Integer>>();
                dateHourExtractMap.put(date,hourExtractMap);
            }
            //遍历每个小时
            for(Map.Entry<String,Long> hourCountEntry :hourCountMap.entrySet()){
                String hour = hourCountEntry.getKey();
                long count = hourCountEntry.getValue();
                //计算每个小时的session数量占当天总session的比例,直接乘每天要抽取的数量,就是当前小时要抽取的数量
                int hourExtractNumber = (int) (((double)count/(double)sessionCount)*extracNumberPerDay);
                if (hourExtractNumber>count){
                    hourExtractNumber = (int)count;
                }
                //先获取当前小时存放随机数的list
                List<Integer> extractIndexList = hourExtractMap.get(hour);
                if (extractIndexList == null){
                    extractIndexList = new ArrayList<Integer>();
                    hourExtractMap.put(hour,extractIndexList);
                }
                //生成上面计算出来的数量的随机数
                for (int i=0;i<hourExtractNumber;i++){
                    int extractIndex =random.nextInt((int)count);

                    while(extractIndexList.contains(extractIndex)){
                        extractIndex =random.nextInt((int)count);
                    }
                    extractIndexList.add(extractIndex);
                }
            }
        }

        /**
         *  fastutil使用,很简单,比如List<Integer>的list,对应到fastutil,就是IntList
         */
        //<date,<hour,(3,5,7,78,109)>>
        //Map<String,Map<String,List<Integer>>>  dateHourExtractMap = new HashMap<String,Map<String,List<Integer>>>();
        Map<String,Map<String,IntList>> fastutilDateHourExtractMap = new HashMap<String, Map<String, IntList>>();
        for(Map.Entry<String,Map<String,List<Integer>>> dateHourExtractEntry : dateHourExtractMap.entrySet()){
            String date = dateHourExtractEntry.getKey();                                         //date
            Map<String, List<Integer>> hourExtracMap = dateHourExtractEntry.getValue();         //<hour,(3,5,7,78,109)>>
            Map<String , IntList> fastutilHourExtractMap = new HashMap<String, IntList>();
            for(Map.Entry<String,List<Integer>> hourExtracEntry: hourExtracMap.entrySet()){
                String hour = hourExtracEntry.getKey();                     //hour
                List<Integer> extractList = hourExtracEntry.getValue();     //(3,5,7,78,109)
                IntList fastutilExtractList = new IntArrayList();
                for (int i=0 ;i<extractList.size() ; i++){
                    fastutilExtractList.add(extractList.get(i));
                }
                fastutilHourExtractMap.put(hour,fastutilExtractList);
            }
            fastutilDateHourExtractMap.put(date,fastutilHourExtractMap);
        }

        /**
         * TODO 广播变量,很简单
         * 其实就是SparkContext的broadcast()方法,传入你要广播的变量,即可
         */
        final Broadcast< Map<String,Map<String,IntList>>> dateHourExtractMapBroadcast = sc.broadcast(fastutilDateHourExtractMap);
        /**
         * 第三步 遍历每天每小时的session,根据随机索引进行抽取
         */
        //执行groupByKey算子,得到<DateHour,(session,aggrInfo)>
        JavaPairRDD<String, Iterable<String>> time2sessionsRDD = time2sessionRDD.groupByKey();
        //我们用flatMap算子,遍历所有的<DateHour,(session,aggrInfo)> 格式的数据
        //会遍历每天每小时的session,如果发现某个session恰巧在我们指定的这天这小时索引上,
        //那么抽取改session写入mysql的random_extract_session中
        //将该抽取的sessionid,返回回来,形成新的javaRDD<String>
        //然后,用抽取出来的sessionid,去join他们访问的访问行为明细数据,写入session表
        JavaPairRDD<String, String> extractSessionidsRDD = time2sessionsRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Iterable<String>>, String, String>() {
            @Override
            public Iterable<Tuple2<String, String>> call(Tuple2<String, Iterable<String>> tuple) throws Exception {
                List<Tuple2<String,String>> extractSessionids = new ArrayList<Tuple2<String,String>>();

                try {
                    String dateHour = tuple._1;
                    String date = dateHour.split("_")[0];
                    String hour = dateHour.split("_")[1];
                    Iterator<String> iterator = tuple._2.iterator();
                    /**
                     * TODO 使用广播变量的时候
                     * 直接调用广播变量(Broadcast类型)的value() / getValue()
                     * 可以获取到之前封装的广播变量
                     */
                    Map<String, Map<String, IntList>> dateHourExtractMap = dateHourExtractMapBroadcast.value();
                    List<Integer> extractIndexList = dateHourExtractMap.get(date).get(hour);

                    ISessionRandomExtractDAO sessionRandomExtractDAO =DAOFactory.getSessionRandomExtractDAO();

                    int index=0;
                    while (iterator.hasNext()){
                        String sessionAggrInfo = iterator.next();
                        //  System.out.println("============================"+sessionAggrInfo+"==========================");
                        if (extractIndexList.contains(index)){
                            String sessionid = StringUtils.getFieldFromConcatString(sessionAggrInfo, "\\|", Constants.FIELD_SESSION_ID);
                            //将数据写入mysql
                            SessionRandomExtract sessionRandomExtract = new SessionRandomExtract();
                            sessionRandomExtract.setTaskid(taskid);
                            sessionRandomExtract.setSessionid(sessionid);
                            sessionRandomExtract.setStartTime(StringUtils.getFieldFromConcatString(sessionAggrInfo,"\\|",Constants.FIELD_START_TIME));
                            sessionRandomExtract.setSearchKeywords(StringUtils.getFieldFromConcatString(sessionAggrInfo,"\\|",Constants.FIELD_SEARCH_KEYWORDS));
                            sessionRandomExtract.setClickCategoryIds(StringUtils.getFieldFromConcatString(sessionAggrInfo,"\\|",Constants.FIELD_CLICK_CATEGORY_IDS));

                            sessionRandomExtractDAO.insert(sessionRandomExtract);

                            //将session加入list
                            extractSessionids.add(new Tuple2<String,String>(sessionid,sessionid));
                        }
                        index++;
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return extractSessionids;
            }
        });
        /**
         * 第四步 获取抽取出来的session的明细数据
         */
        JavaPairRDD<String, Tuple2<String, Row>> extraxtSessionDetailRDD = extractSessionidsRDD.join(sessionid2ActionRDD);
        extraxtSessionDetailRDD.foreach(new VoidFunction<Tuple2<String, Tuple2<String, Row>>>(){
            @Override
            public void call(Tuple2<String, Tuple2<String, Row>> tuple) throws Exception {
                Row row = tuple._2._2;
                // System.out.println("============================"+row+"==========================");
                SessionDetail sessionDetail = new SessionDetail();
                sessionDetail.setTaskid(taskid);
                sessionDetail.setUserid(row.getLong(1));
                sessionDetail.setSessionid(row.getString(2));
                sessionDetail.setPageid(row.getLong(3));
                sessionDetail.setActionTime(row.getString(4));
                sessionDetail.setSearchKeyword(row.getString(5));
                sessionDetail.setClickCategoryId(row.getLong(6));
                sessionDetail.setClickProductId(row.getLong(7));
                sessionDetail.setOrderCategoryIds(row.getString(8));
                sessionDetail.setOrderProductIds(row.getString(9));
                sessionDetail.setPayCategoryIds(row.getString(10));
                sessionDetail.setPayProductIds(row.getString(11));

                ISessionDetailDAO sessionDetailDAO = DAOFactory.getSessionDetailDAO();
                sessionDetailDAO.insert(sessionDetail);
            }
        });
    }
    /**
     * 计算session范围占比,并且用mysql
     * @param value
     */
    private static void calulateAndPersistAggrSet(String value,long taskid){
        //  System.out.println("============================"+value+"==========================");
        //从Accumulator统计串中获取值
        long session_count = Long.valueOf(StringUtils.getFieldFromConcatString(value,"\\|",Constants.SESSION_COUNT));
        long visit_length_1s_3s = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_1s_3s));
        long visit_length_4s_6s = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_4s_6s));
        long visit_length_7s_9s = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_7s_9s));
        long visit_length_10s_30s = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_10s_30s));
        long visit_length_30s_60s = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_30s_60s));
        long visit_length_1m_3m = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_1m_3m));
        long visit_length_3m_10m = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_3m_10m));
        long visit_length_10m_30m = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_10m_30m));
        long visit_length_30m = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_30m));

        long step_length_1_3 = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_1_3));
        long step_length_4_6 = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_4_6));
        long step_length_7_9 = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_7_9));
        long step_length_10_30 = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_10_30));
        long step_length_30_60 = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_30_60));
        long step_length_60 = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_60));

        // 计算各个访问时长和访问步长的范围
        double visit_length_1s_3s_ratio      = NumberUtils.formatDouble((double)visit_length_1s_3s / (double)session_count, 2);
        double visit_length_4s_6s_ratio      = NumberUtils.formatDouble((double)visit_length_4s_6s / (double)session_count, 2);
        double visit_length_7s_9s_ratio     = NumberUtils.formatDouble((double)visit_length_7s_9s / (double)session_count, 2);
        double visit_length_10s_30s_ratio    = NumberUtils.formatDouble((double)visit_length_10s_30s / (double)session_count, 2);
        double visit_length_30s_60s_ratio   = NumberUtils.formatDouble((double)visit_length_30s_60s / (double)session_count, 2);
        double visit_length_1m_3m_ratio      = NumberUtils.formatDouble((double)visit_length_1m_3m / (double)session_count, 2);
        double visit_length_3m_10m_ratio    = NumberUtils.formatDouble((double)visit_length_3m_10m / (double)session_count, 2);
        double visit_length_10m_30m_ratio   = NumberUtils.formatDouble((double)visit_length_10m_30m / (double)session_count, 2);
        double visit_length_30m_ratio        = NumberUtils.formatDouble((double)visit_length_30m / (double)session_count, 2);

//        double visit_length_1s_3s_ratio      = visit_length_1s_3s;
//        double visit_length_4s_6s_ratio      = visit_length_4s_6s;
//        double visit_length_7s_9s_ratio     = visit_length_7s_9s;
//        double visit_length_10s_30s_ratio    = visit_length_10s_30s;
//        double visit_length_30s_60s_ratio   = visit_length_30s_60s;
//        double visit_length_1m_3m_ratio      = visit_length_1m_3m;
//        double visit_length_3m_10m_ratio    = visit_length_3m_10m;
//        double visit_length_10m_30m_ratio   = visit_length_10m_30m;
//        double visit_length_30m_ratio        =visit_length_30m;


        double step_length_1_3_ratio    = NumberUtils.formatDouble((double)step_length_1_3 / (double)session_count, 2);
        double step_length_4_6_ratio    = NumberUtils.formatDouble((double)step_length_4_6 / (double)session_count, 2);
        double step_length_7_9_ratio    = NumberUtils.formatDouble((double)step_length_7_9 / (double)session_count, 2);
        double step_length_10_30_ratio  = NumberUtils.formatDouble((double)step_length_10_30 / (double)session_count, 2);
        double step_length_30_60_ratio  = NumberUtils.formatDouble((double)step_length_30_60 / (double)session_count, 2);
        double step_length_60_ratio     = NumberUtils.formatDouble((double)step_length_60 / (double)session_count, 2);

        //将结果封装为Domain对象
        SessionAggrStat sessionAggrStat = new SessionAggrStat();
        sessionAggrStat.setTaskid(taskid);
        sessionAggrStat.setSession_count(session_count);
        sessionAggrStat.setVisit_length_1s_3s_ratio(visit_length_1s_3s_ratio);
        sessionAggrStat.setVisit_length_4s_6s_ratio(visit_length_4s_6s_ratio);
        sessionAggrStat.setVisit_length_7s_9s_ratio(visit_length_7s_9s_ratio);
        sessionAggrStat.setVisit_length_10s_30s_ratio(visit_length_10s_30s_ratio);
        sessionAggrStat.setVisit_length_30s_60s_ratio(visit_length_30s_60s_ratio);
        sessionAggrStat.setVisit_length_1m_3m_ratio(visit_length_1m_3m_ratio);
        sessionAggrStat.setVisit_length_3m_10m_ratio(visit_length_3m_10m_ratio);
        sessionAggrStat.setVisit_length_10m_30m_ratio(visit_length_10m_30m_ratio);
        sessionAggrStat.setVisit_length_30m_ratio(visit_length_30m_ratio);
        sessionAggrStat.setStep_length_1_3_ratio(step_length_1_3_ratio);
        sessionAggrStat.setStep_length_4_6_ratio(step_length_4_6_ratio);
        sessionAggrStat.setStep_length_7_9_ratio(step_length_7_9_ratio);
        sessionAggrStat.setStep_length_10_30_ratio(step_length_10_30_ratio);
        sessionAggrStat.setStep_length_30_60_ratio(step_length_30_60_ratio);
        sessionAggrStat.setStep_length_60_ratio(step_length_60_ratio);

        //调用对应的DAO插入统计结果
        ISessionAggrStatDAO sessionAggrStatDAO = DAOFactory.getSessionAggrStatDAO();
        sessionAggrStatDAO.insert(sessionAggrStat);
    }

    /**
     * TODO 获取top10热门品类
     * //@param filteredSessionid2AggrInfoRDD   过滤100个sesison聚合 |
     * //@param sessionid2ActionRDD               全部session的原始的值
     */
    private static List<Tuple2<CategorySortKey,String>> getTop10Category(Long taskid, JavaPairRDD<String, Row> sessionid2detailRDD){
        //第一步 获取符合条件的session访问过的所有品类
        //获取符合条件的session的访问明细
        //移到公共 getSessionid2detailRDD 方法中
//        JavaPairRDD<String,Row> sessionid2detailRDD = filteredSessionid2AggrInfoRDD.join(sessionid2ActionRDD).mapToPair(new PairFunction<Tuple2<String, Tuple2<String, Row>>, String, Row>(){
//            @Override
//            public Tuple2<String, Row> call(Tuple2<String, Tuple2<String, Row>> tuple) throws Exception {
//               //System.out.println("============================"+tuple._1+"========"+tuple._2._1+"======"+tuple._2._2+"==========================");
//                return new Tuple2<String, Row>(tuple._1,tuple._2._2);
//            }
//        });
        // System.out.println("============================"+filteredSessionid2AggrInfoRDD.count()+"================="+sessionid2detailRDD.count()+"==================");
        //获取session访问过的所有品类id
        //访问过指的是点击过,下单过,支付过的品类
        JavaPairRDD<Long,Long> categoryidRDD = sessionid2detailRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Row>, Long, Long>(){
            @Override
            public Iterable<Tuple2<Long, Long>> call(Tuple2<String, Row> tuple) throws Exception {
                Row row = tuple._2;

                List<Tuple2<Long,Long>> list = new ArrayList<Tuple2<Long, Long>>();

                Long clickCategoryId = row.getLong(6);
                if (clickCategoryId != null){
                    list.add(new Tuple2<Long, Long>(clickCategoryId,clickCategoryId));
                }
                String orderCategoryIds = row.getString(8);
                if (orderCategoryIds != null){
                    String[] orderCategoryIdsSplited = orderCategoryIds.split(",");
                    for (String orderCategoryId : orderCategoryIdsSplited){
                        list.add(new Tuple2<Long, Long>(Long.valueOf(orderCategoryId),Long.valueOf(orderCategoryId)));
                    }
                }
                String payCategoryIds = row.getString(10);
                if (payCategoryIds != null){
                    String[] payCategoryIdsSplited = payCategoryIds.split(",");
                    for(String payCategoryId : payCategoryIdsSplited){
                        list.add(new Tuple2<Long, Long>(Long.valueOf(payCategoryId),Long.valueOf(payCategoryId)));
                    }

                }
                return list;
            }
        });
        /**
         * 必须要进行去重,如果不去重会出现重复的categoryid
         */
        categoryidRDD =  categoryidRDD.distinct();
        /**
         * 第二步 计算各品类点击下单支付的次数
         */
        //访问明细中,其中三种访问行为是:点击,下单,支付
        //分别来计算各品类点击,下单,支付的次数,可以先对访问明细数据进行过滤
        //分别过滤出点击,下单,支付行为,然后通过map,reduceByKey等算子来进行计算

        //计算各个品类的点击次数
        JavaPairRDD<Long, Long> clickCategoryId2CountRDD =getClickCategoryId2CountRDD(sessionid2detailRDD);
        //计算各个品类的下单次数
        JavaPairRDD<Long, Long> orderCategoryId2CountRDD =getOrderCategoryId2CountRDD(sessionid2detailRDD);
        //计算各个品类的支付次数
        JavaPairRDD<Long, Long> payCategoryId2CountRDD =getPayCategoryId2CountRDD(sessionid2detailRDD);
        /**
         * 第三步 join各个品类与他的点击,下单,支付的次数
         *
         * categoryidRDD中,包含了所有的符合条件的session,可能不是包含所有品类的id
         *
         * 上面分别计算出来的三份,各品类的点击,下单和支付的次数,可能不是包含所有品类的
         * 比如有的品类就只是被点击过,没有人下单支付
         * 所以,这里,就不能是用join,要使用leftjoin操作,就是,如果categoryidRDD不能join到自己某个数据
         * 比如,点击或下单或支付次数,那么该categoryidRDD还是要保留下来的
         * 只不过没有join到的数据就是0了
         */
        JavaPairRDD<Long, String> categoryid2countRDD = joinCategoryAndData(categoryidRDD, clickCategoryId2CountRDD, orderCategoryId2CountRDD, payCategoryId2CountRDD);
        /**
         * 第四步 自定义二次排序的key
         */

        /**
         * 第五步 将数据映射成<CategorySortKey,info>格式的RDD,然后进行二次排序(降序)
         */
        JavaPairRDD<CategorySortKey,String> sortKey2countRDD = categoryid2countRDD.mapToPair(new PairFunction<Tuple2<Long, String>, CategorySortKey, String>(){
            @Override
            public Tuple2<CategorySortKey, String> call(Tuple2<Long, String> tuple) throws Exception {
                String countInfo = tuple._2;
                long clickCount = Long.valueOf(StringUtils.getFieldFromConcatString(countInfo,"\\|",Constants.FIELD_CLICK_COUNT));
                long orderCount = Long.valueOf(StringUtils.getFieldFromConcatString(countInfo,"\\|",Constants.FIELD_ORDER_COUNT));
                long payCount =   Long.valueOf(StringUtils.getFieldFromConcatString(countInfo,"\\|",Constants.FIELD_PAY_COUNT));

                CategorySortKey sortKey = new CategorySortKey(clickCount,orderCount,payCount);
                // System.out.println("=====000======================="+sortKey+"======="+countInfo+"===================");
                return new Tuple2<CategorySortKey, String>(sortKey,countInfo);
            }
        });
        JavaPairRDD<CategorySortKey, String> sortedCategoryCountRDD = sortKey2countRDD.sortByKey(false);
        /**
         *第六步 用take(10) 取出top10热门品类,并写入mysql
         */
        ITop10CategoryDAO top10CategoryDAO = DAOFactory.getTop10CategoryDAO();
        List<Tuple2<CategorySortKey, String>> top10CategoryList = sortedCategoryCountRDD.take(10);
        for(Tuple2<CategorySortKey, String> tuple : top10CategoryList){
            String countInfo = tuple._2;
            long categoryid = Long.valueOf(StringUtils.getFieldFromConcatString(countInfo,"\\|",Constants.FIELD_CATEGOTY_ID));
            long clickCount = Long.valueOf(StringUtils.getFieldFromConcatString(countInfo,"\\|",Constants.FIELD_CLICK_COUNT));
            long orderCount = Long.valueOf(StringUtils.getFieldFromConcatString(countInfo,"\\|",Constants.FIELD_ORDER_COUNT));
            long payCount = Long.valueOf(StringUtils.getFieldFromConcatString(countInfo,"\\|",Constants.FIELD_PAY_COUNT));

            Top10Category category = new Top10Category();
            category.setTaskid(taskid);
            category.setCategoryid(categoryid);
            category.setClickCount(clickCount);
            category.setOrderCount(orderCount);
            category.setPayCount(payCount);

            top10CategoryDAO.insert(category);
        }
        return  top10CategoryList;
    }

    /**
     * TODO 获取各个品类点击次数RDD
     * @return
     */
    private static JavaPairRDD<Long,Long> getClickCategoryId2CountRDD(JavaPairRDD<String,Row> sessionid2detailRDD){
        /**
         * 说明一下
         * 这里,是对完整数据进行了filter过滤,过滤出来的行为数据
         * 点击行为数据其实只占一小部分
         * 所以过滤后的RDD,每个partition数据量,很有可能很不均匀
         *
         * 针对这种情况,适合使用coalesce算子的,在filter过后去减少partition的数量
         */
        JavaPairRDD<String, Row> clickActionRDD = sessionid2detailRDD.filter(new Function<Tuple2<String, Row>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, Row> tuple) throws Exception {
                Row row = tuple._2;
                //System.out.println("000============================"+row.getLong(6)+"==========================");
                return row.get(6) != null ? true:false;
            }
        });
                //.coalesce(100); //TODO 对这个操作说明,本地用的是local模式,不用设置分区跟并行度数量,local模式本身进程内模拟,性能很高,而且对并行度,partition内部有优化,自己设置画蛇添足
        JavaPairRDD<Long, Long> clickCategoryIdRDD = clickActionRDD.mapToPair(new PairFunction<Tuple2<String, Row>, Long, Long>() {
            @Override
            public Tuple2<Long, Long> call(Tuple2<String, Row> tuple) throws Exception {
                long clickCategoruId = tuple._2.getLong(6);
                // System.out.println("1111============================"+clickCategoruId+"==========================");
                return new Tuple2<Long, Long>(clickCategoruId,1L);
            }
        });
        /**
         * 计算各个品类的点击次数
         * 如果某个品类点击了1000W次.其他10W次,会数据倾斜
         */
        JavaPairRDD<Long, Long> clickCategoryId2CountRDD = clickCategoryIdRDD.reduceByKey(new Function2<Long, Long, Long>() {
            @Override
            public Long call(Long v1, Long v2) throws Exception {
                return v1+v2;
            }
        });
        /**
         * TODO 提升shuffer  reduce端并行度
         */
//        JavaPairRDD<Long, Long> clickCategoryId2CountRDD = clickCategoryIdRDD.reduceByKey(new Function2<Long, Long, Long>() {
//            @Override
//            public Long call(Long v1, Long v2) throws Exception {
//                return v1+v2;
//            }
//        },1000);
//        /**
//         * TODO 使用随机key实现双重聚合
//         * TODO 第一步,给每个key打上随机数,
//         */
//        JavaPairRDD<String, Long> mappedClickCategoryIdRDD = clickCategoryId2CountRDD.mapToPair(new PairFunction<Tuple2<Long, Long>, String, Long>() {
//            @Override
//            public Tuple2<String, Long> call(Tuple2<Long, Long> tuple) throws Exception {
//                Random random = new Random();
//                int prefix = random.nextInt(10);
//                return new Tuple2<String, Long>(prefix+"_"+tuple._1,tuple._2);
//            }
//        });
//        /**
//         * TODO 第二步,执行第一步局部聚合
//         */
//        JavaPairRDD<String, Long> firstAggrRDD = mappedClickCategoryIdRDD.reduceByKey(new Function2<Long, Long, Long>() {
//            @Override
//            public Long call(Long v1, Long v2) throws Exception {
//                return v1+v2;
//            }
//        });
//        /**
//         * TODO 第三步,去除掉key的前缀
//         */
//        JavaPairRDD<Long, Long> restoreRDD = firstAggrRDD.mapToPair(new PairFunction<Tuple2<String, Long>, Long, Long>() {
//            @Override
//            public Tuple2<Long, Long> call(Tuple2<String, Long> tuple) throws Exception {
//                long categoryId = Long.valueOf(tuple._1.split("_")[2]);
//                return new Tuple2<Long, Long>(categoryId,tuple._2);
//            }
//        });
//        /**
//         * TODO 第四步,做第二轮全局的聚合
//         */
//        JavaPairRDD<Long, Long> globalAggrRDD = restoreRDD.reduceByKey(new Function2<Long, Long, Long>() {
//            @Override
//            public Long call(Long v1, Long v2) throws Exception {
//                return v1+v2;
//            }
//        });

        return clickCategoryId2CountRDD;
    }
    /**
     * TODO 获取各个品类下单次数RDD
     * @return
     */
    private static JavaPairRDD<Long,Long> getOrderCategoryId2CountRDD(JavaPairRDD<String,Row> sessionid2detailRDD){
        JavaPairRDD<String, Row> orderActionRDD = sessionid2detailRDD.filter(new Function<Tuple2<String, Row>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, Row> tuple) throws Exception {
                Row row = tuple._2;
                //System.out.println("000============================"+row.getLong(6)+"==========================");
                return row.getString(8) != null ? true:false;
            }
        });
        JavaPairRDD<Long, Long> orderCategoryIdRDD = orderActionRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Row>, Long, Long>(){
            @Override
            public Iterable<Tuple2<Long, Long>> call(Tuple2<String, Row> tuple) throws Exception {
                Row row = tuple._2;
                String orderCategoryIds = row.getString(8);
                String[] orderCategoryIdsSplited = orderCategoryIds.split(",");
                List<Tuple2<Long,Long>> list = new ArrayList<Tuple2<Long,Long>>();
                for (String orderCategoryId : orderCategoryIdsSplited){
                    list.add(new Tuple2<Long, Long>(Long.valueOf(orderCategoryId),1L));
                }
                return list;
            }
        });
        JavaPairRDD<Long, Long> orderCategoryId2CountRDD = orderCategoryIdRDD.reduceByKey(new Function2<Long, Long, Long>() {
            @Override
            public Long call(Long v1, Long v2) throws Exception {
                return v1+v2;
            }
        });
        return orderCategoryId2CountRDD;
    }
    /**
     * TODO 获取各个品类支付次数RDD
     * @return
     */
    private static JavaPairRDD<Long,Long> getPayCategoryId2CountRDD(JavaPairRDD<String,Row> sessionid2detailRDD){
        JavaPairRDD<String, Row> payActionRDD = sessionid2detailRDD.filter(new Function<Tuple2<String, Row>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, Row> tuple) throws Exception {
                Row row = tuple._2;
                //System.out.println("000============================"+row.getLong(6)+"==========================");
                return row.getString(10) != null ? true:false;
            }
        });
        JavaPairRDD<Long, Long> payCategoryIdRDD = payActionRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Row>, Long, Long>(){
            @Override
            public Iterable<Tuple2<Long, Long>> call(Tuple2<String, Row> tuple) throws Exception {
                Row row = tuple._2;
                String orderCategoryIds = row.getString(10);
                String[] payCategoryIdsSplited = orderCategoryIds.split(",");
                List<Tuple2<Long,Long>> list = new ArrayList<Tuple2<Long,Long>>();
                for (String payCategoryId : payCategoryIdsSplited){
                    list.add(new Tuple2<Long, Long>(Long.valueOf(payCategoryId),1L));
                }
                return list;
            }
        });
        JavaPairRDD<Long, Long> payCategoryId2CountRDD = payCategoryIdRDD.reduceByKey(new Function2<Long, Long, Long>() {
            @Override
            public Long call(Long v1, Long v2) throws Exception {
                return v1+v2;
            }
        });
        return payCategoryId2CountRDD;
    }

    /**
     * TODO 连接品类RDD与数据RDD
     * @param categoryidRDD
     * @param clickCategoryId2CountRDD
     * @param orderCategoryId2CountRDD
     * @param payCategoryId2CountRDD
     * @return
     */
    private static JavaPairRDD<Long,String> joinCategoryAndData(
            JavaPairRDD<Long,Long> categoryidRDD,
            JavaPairRDD<Long,Long> clickCategoryId2CountRDD,
            JavaPairRDD<Long,Long> orderCategoryId2CountRDD,
            JavaPairRDD<Long,Long> payCategoryId2CountRDD){
        //如果用leftOuterJoin,就可能出现,右边那个RDD中,join过来时,没有值
        //所以Tuole中的第二个值用Optional<Long>类型,就代表,可能有值,可能没有值
        JavaPairRDD<Long, Tuple2<Long, Optional<Long>>> tmpJoinRDD = categoryidRDD.leftOuterJoin(clickCategoryId2CountRDD);
        //左外连接点击次数
        JavaPairRDD<Long, String> tmpMapRDD = tmpJoinRDD.mapToPair(new PairFunction<Tuple2<Long, Tuple2<Long, Optional<Long>>>, Long, String>() {
            @Override
            public Tuple2<Long, String> call(Tuple2<Long, Tuple2<Long, Optional<Long>>> tuple) throws Exception {
                long categoryid = tuple._1;
                Optional<Long> clickCountOptional = tuple._2._2;
                long clickCount=0L;
                if (clickCountOptional.isPresent()){//如果有值
                    clickCount = clickCountOptional.get();
                }
                String value =Constants.FIELD_CATEGOTY_ID+"="+categoryid+"|"+Constants.FIELD_CLICK_COUNT +"="+clickCount;
                //   System.out.println("============================"+value+"==========================");
                return new Tuple2<Long, String>(categoryid,value);
            }
        });
        //左外连接下单次数
        tmpMapRDD = tmpMapRDD.leftOuterJoin(orderCategoryId2CountRDD).mapToPair(new PairFunction<Tuple2<Long, Tuple2<String, Optional<Long>>>, Long, String>(){
            @Override
            public Tuple2<Long, String> call(Tuple2<Long, Tuple2<String, Optional<Long>>> tuple) throws Exception {
                long categoryid = tuple._1;
                String value = tuple._2._1;
                Optional<Long> orderCountOptional = tuple._2._2;
                long orderCount=0L;
                if (orderCountOptional.isPresent()){//如果有值
                    orderCount = orderCountOptional.get();
                }
                //   System.out.println("================"+categoryid+"==================="+value+"==============="+orderCount+"==================");
                value =value+"|" +Constants.FIELD_ORDER_COUNT+"="+orderCount;
                //   System.out.println("============================"+value+"==========================");
                return new Tuple2<Long, String>(categoryid,value);
            }
        });
        //左外连接支付次数
        tmpMapRDD = tmpMapRDD.leftOuterJoin(payCategoryId2CountRDD).mapToPair(new PairFunction<Tuple2<Long, Tuple2<String, Optional<Long>>>, Long, String>(){
            @Override
            public Tuple2<Long, String> call(Tuple2<Long, Tuple2<String, Optional<Long>>> tuple) throws Exception {
                long categoryid = tuple._1;
                String value = tuple._2._1;
                Optional<Long> payCountOptional = tuple._2._2;
                long payCount=0L;
                if (payCountOptional.isPresent()){//如果有值
                    payCount = payCountOptional.get();
                }
                value =value +"|" +Constants.FIELD_PAY_COUNT+"="+payCount;
                //  System.out.println("============================"+value+"==========================");
                return new Tuple2<Long, String>(categoryid,value);
            }
        });
        return tmpMapRDD;
    }

    /**
     * 获取top10 活跃session
     * @param taskid
     * @param sessionid2detailRDD
     */
    private static void getTop10Session(JavaSparkContext sc , final long taskid, List<Tuple2<CategorySortKey,String>> top10CategoryList, JavaPairRDD<String, Row> sessionid2detailRDD) {
        /**
         * 第一步  将top10热门品类的id,生成一份RDD
         */
        List<Tuple2<Long,Long>> top10CategoryIdList = new ArrayList<Tuple2<Long, Long>>();
        for (Tuple2<CategorySortKey,String> category : top10CategoryList){
            long categoryid = Long.valueOf(StringUtils.getFieldFromConcatString(category._2,"\\|",Constants.FIELD_CATEGOTY_ID));
            //System.out.println("============================"+categoryid+"=======================");
            top10CategoryIdList.add(new Tuple2<Long, Long>(categoryid,categoryid));
        }
        //<categoryid,categoryid>
        JavaPairRDD<Long, Long> top10CategoryIdRDD = sc.parallelizePairs(top10CategoryIdList, 1);

        /**
         * 第二步 计算top10各个品类被个session点击的次数
         */
        //<sessionid,Iterable<Row>>
        JavaPairRDD<String, Iterable<Row>> sessionid2detailsRDD = sessionid2detailRDD.groupByKey();
        //返回<categoryid,<sessionid,Count>>
        JavaPairRDD<Long,String> categoryid2sessionCountRDD = sessionid2detailsRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Iterable<Row>>, Long, String>(){
            @Override
            public Iterable<Tuple2<Long, String>> call(Tuple2<String, Iterable<Row>> tuple) throws Exception {
                String sessionid = tuple._1;    //sessionid
                Iterator<Row> iterator = tuple._2.iterator();   //Row
                HashMap<Long, Long> categoryCountMap = new HashMap<Long, Long>();

                //计算出改session,对每个品类的点击次数
                while(iterator.hasNext()){
                    Row row = iterator.next();
                    if (row.get(6) != null ){
                        long categoryid = row.getLong(6);
                        Long count = categoryCountMap.get(categoryid);
                        //  System.out.println("11111============"+categoryid+"================="+count+"==========================");
                        if (count== null){
                            count =0L;
                        }
                        count++;
                        ///System.out.println("000============================"+categoryid+"====================="+count+"==================");
                        categoryCountMap.put(categoryid,count);
                    }
                }
                //返回结果<categoryuid,<sessionid,Count>>格式
                List<Tuple2<Long,String>> list = new ArrayList<Tuple2<Long, String>>();
                for (Map.Entry<Long,Long> categoryCountEntry : categoryCountMap.entrySet()){
                    long categoryid = categoryCountEntry.getKey();
                    long count = categoryCountEntry.getValue();
                    String value = sessionid + "," +count;
                   /// System.out.println("111============================"+categoryid+"=================="+value+"==============");
                    list.add(new Tuple2<Long, String>(categoryid,value));
                }
                return list;
            }
        });
        //获取到top10热门品类,对各个session点击的次数 <categoryId,<sessionid,Count>>
        JavaPairRDD<Long, String> top10CategorySessionCountRDD = top10CategoryIdRDD.join(categoryid2sessionCountRDD).mapToPair(new PairFunction<Tuple2<Long, Tuple2<Long, String>>, Long, String>(){
            @Override
            public Tuple2<Long, String> call(Tuple2<Long, Tuple2<Long, String>> tuple) throws Exception {
                return new Tuple2<Long, String>(tuple._1,tuple._2._2);
            }
        });

        /**
         * 第三步 分组取TopN算法实现,获取每个品类top10活跃用户
         */
        //<categoruid ,Iterable<sessionid,Count> >
        JavaPairRDD<Long, Iterable<String>> top10CategorySessionCountsRDD = top10CategorySessionCountRDD.groupByKey();

        JavaPairRDD<String, String> top10SessionRDD = top10CategorySessionCountsRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<Long, Iterable<String>>, String, String>() {
            @Override
            public Iterable<Tuple2<String, String>> call(Tuple2<Long, Iterable<String>> tuple) throws Exception {
                long categoryid = tuple._1; //categoruid
                Iterator<String> iterator = tuple._2.iterator(); //Iterable<count>

                //定义取TopN的排序数组
                String [] top10Sessions = new String[10];

                while (iterator.hasNext()){
                    String sessionCount = iterator.next();                          //sessionid,Count
                    String sessionid =sessionCount.split(",")[0];           //sessionid
                    long  count =Long.valueOf(sessionCount.split(",")[1]);  //Count

                    //遍历排序数组
                    for (int i=0 ; i<top10Sessions.length; i++){
                        //如果当前i位,没有数据,直接将i位数据赋值为sessionCount
                        if (top10Sessions[i] == null){
                            top10Sessions[i] =sessionCount;
                            break;
                        }else{
                            long _count = Long.valueOf(top10Sessions[i].split(",")[1]);
                            //如果sessionCount比i位的sessionCount要大
                            if (count > _count){
                                //从排序数组最后一位开始到i位,所有数据往后挪一位
                                for (int j=9 ; j>i ;j--){
                                    top10Sessions[j] = top10Sessions[j-1];
                                }
                                //将i位赋值为sessionCount
                                top10Sessions[i] = sessionCount;
                                break;
                            }

                            //比较小,继续外层for循环
                        }
                    }
                }
                //将数据写入mysql表
                List<Tuple2<String,String>> list  = new ArrayList<Tuple2<String, String>>();
                for (String sessionCount : top10Sessions){
                    if (sessionCount != null) {
                        String sessionid = sessionCount.split(",")[0];
                        long count = Long.valueOf(sessionCount.split(",")[1]);

                        //将top10 session插入到mysql表
                        Top10Session top10Session = new Top10Session();
                        top10Session.setTaskid(taskid);
                        top10Session.setCategoryid(categoryid);
                        top10Session.setSessionid(sessionid);
                        top10Session.setClickCount(count);

                        ITop10SessionDAO top10SessionDAO = DAOFactory.getTop10SessionDAO();
                        top10SessionDAO.insert(top10Session);

                        //放入list中
                        list.add(new Tuple2<String, String>(sessionid, sessionid));
                    }
                }

                return list;
            }
        });

        /**
         * 第四补 获取top10活跃session的明细数据,并写入mysql
         */
        JavaPairRDD<String, Tuple2<String, Row>> sessionDetailRDD = top10SessionRDD.join(sessionid2detailRDD);
//        sessionDetailRDD.foreach(new VoidFunction<Tuple2<String, Tuple2<String, Row>>>(){
//            @Override
//            public void call(Tuple2<String, Tuple2<String, Row>> tuple) throws Exception {
//                Row row = tuple._2._2;
//                SessionDetail sessionDetail = new SessionDetail();
//                sessionDetail.setTaskid(taskid);
//                sessionDetail.setUserid(row.getLong(1));
//                sessionDetail.setSessionid(row.getString(2));
//                sessionDetail.setPageid(row.getLong(3));
//                sessionDetail.setActionTime(row.getString(4));
//                sessionDetail.setSearchKeyword(row.getString(5));
//                sessionDetail.setClickCategoryId(row.getLong(6));
//                sessionDetail.setClickProductId(row.getLong(7));
//                sessionDetail.setOrderCategoryIds(row.getString(8));
//                sessionDetail.setOrderProductIds(row.getString(9));
//                sessionDetail.setPayCategoryIds(row.getString(10));
//                sessionDetail.setPayProductIds(row.getString(11));
//
//                ISessionDetailDAO sessionDetailDAO = DAOFactory.getSessionDetailDAO();
//                sessionDetailDAO.insert(sessionDetail);
//            }
//        });
        sessionDetailRDD.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Tuple2<String, Row>>>>(){
            @Override
            public void call(Iterator<Tuple2<String, Tuple2<String, Row>>> iterator) throws Exception {
                List<SessionDetail> sessionDetails = new ArrayList<SessionDetail>();
                while(iterator.hasNext()){
                     Tuple2<String, Tuple2<String, Row>> tuple = iterator.next();
                     Row row = tuple._2._2;
                     SessionDetail sessionDetail = new SessionDetail();
                     sessionDetail.setTaskid(taskid);
                     sessionDetail.setUserid(row.getLong(1));
                     sessionDetail.setSessionid(row.getString(2));
                     sessionDetail.setPageid(row.getLong(3));
                     sessionDetail.setActionTime(row.getString(4));
                     sessionDetail.setSearchKeyword(row.getString(5));
                     sessionDetail.setClickCategoryId(row.getLong(6));
                     sessionDetail.setClickProductId(row.getLong(7));
                     sessionDetail.setOrderCategoryIds(row.getString(8));
                     sessionDetail.setOrderProductIds(row.getString(9));
                     sessionDetail.setPayCategoryIds(row.getString(10));
                     sessionDetail.setPayProductIds(row.getString(11));

                    sessionDetails.add(sessionDetail);
                }
                ISessionDetailDAO sessionDetailDAO = DAOFactory.getSessionDetailDAO();
                sessionDetailDAO.insertBatch(sessionDetails);
            }
        });

    }
}
