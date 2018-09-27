package com.ibeifeng.sparkproject.spark.ad;

import com.google.common.base.*;
import com.google.common.base.Optional;
import com.ibeifeng.sparkproject.conf.ConfigurationManager;
import com.ibeifeng.sparkproject.constant.Constants;
import com.ibeifeng.sparkproject.dao.*;
import com.ibeifeng.sparkproject.dao.factory.DAOFactory;
import com.ibeifeng.sparkproject.domain.*;
import com.ibeifeng.sparkproject.jdbc.JDBCHelper;
import com.ibeifeng.sparkproject.util.DateUtils;
import javafx.util.Duration;
import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.*;

/**
 *
 */
public class AdClickRealTimeStartSpark {

    public static void main(String[] args) {
        //构建spark上下文
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("AdClickRealTimeStartSpark")
               // .set("spark.streaming.receiver.writeAheadLog.enable" ,"true")
               // .set("spark.streaming.blockInterval","50")
                //.set("spark.default.parallelism","1000")
                ;

        //spark streaming的上下文是构建JavaStreamingContext对象
        //而不是像之前的JavaSparkContext,SQLContext/HiveContext
        //传入的第一个参数,和之前的spark上下文一样,也是SparkConf对象;第二个参数则不一样

        //第二个参数是spark streaming类型作业比较有特色的一个参数
        //实时处理batch的interval
        //spark streaming  ,每隔一小段时间会去收集一次数据源中的数据,做成一个batch
        //每次都是处理一个batch中的数据

        //通常来说,batch interval,就是每隔多少时间收集一次数据源中的数据,然后进行处理,
        //一般spark streaming应用,都是设置数秒到数十秒(很少超过1分钟)

        //咱们这里项目中,就设置5秒钟的batch interval
        //每隔5秒钟,咱们的spark streaming作业就会手机最近5秒内的数据源接收过来的数据
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));
        jssc.checkpoint("hdfs://lkq:50070/streaming_checkpoint");

        //正式开始代码编写
        //实现业务逻辑功能

        //创建针对kafka数据来源的数据DStream(离线流,代表一个源源不断的数据来源,抽象)
        //选用kafka direct api(很多好处,包括自己内部自适应每次接收数据量的特性,等等)

        //构建kafka参数map
        //主要放置的就是,你要连接的kafka集群的地址(broker集群的地址列表)
        Map<String, String> kafkaParams = new HashMap<String, String>();
        kafkaParams.put("metadata.broker.list", ConfigurationManager.getProperty(Constants.KAFKA_METADATA_BROKER_LIST));//放入kafka地址

        //构建topic set
        String kafkaTopics = ConfigurationManager.getProperty(Constants.KAFKA_TOPICS);
        String[] kafkatopicsSplited = kafkaTopics.split(",");

        final Set<String> topics = new HashSet<String>();
        for (String kafkatopic : kafkatopicsSplited) {
            topics.add(kafkatopic);
        }

        //基于kafka direct api模式,构建出针对kafka集群中指定的topic的数据DStream
        //两个值,val1,val2,val1没有什么意义,val2包含了kafka topic中的一条一条的实时日志数据
        final JavaPairInputDStream<String, String> adRealTimeLogDStream = KafkaUtils.createDirectStream(
                jssc,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                kafkaParams,
                topics);
       // adRealTimeLogDStream.repartition(1000);
        //TODO 过滤黑名单数据
        JavaPairDStream<String, String> filteredADRealTimeLogDStream = filterByBlacklist(adRealTimeLogDStream);

        //TODO 生成动态黑名单
        generateDynamicBlacklist(filteredADRealTimeLogDStream);

        //TODO 业务功能一: 计算广告点击流量实时统计结果(yyyyMMdd_provinve_city_adid,clickCount)
        JavaPairDStream<String, Long> adRealTimeStatDStream = calculateRealTimeStat(filteredADRealTimeLogDStream);

        //TODO 业务功能二: 实时统计每天每个省份top3热门广告
        calculateProvinceTop3Ad(adRealTimeStatDStream);
        //TODO 业务功能三: 实时统计每天每个广告在最近1小时的滑动窗口内的点击趋势(每分钟的点击量)
        //我们每次都可以看到每个广告最近一小时内每分钟的点击量,每只广告的点击趋势
        calculateAdClickByWindow(adRealTimeLogDStream);

        //TODO 构建完spark streaming上下文之后,记得要进行上下文启动,等待执行结束,关闭
        jssc.start();
        jssc.awaitTermination();
        jssc.close();
    }

//    /**
//     * TODO 使用cluster模式提交
//     * TODO 使用cluster模式提交
//     * TODO 使用cluster模式提交
//     * TODO 使用cluster模式提交
//     */
//    @SuppressWarnings("unused")
//    private static void testDriverHA(){
//        final String checkPointDir = "hdfs://lkq:50070/streaming_checkpoint";
//        JavaStreamingContextFactory contextFactory = new JavaStreamingContextFactory() {
//            @Override
//            public JavaStreamingContext create() {
//                SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("AdClickRealTimeStartSpark");
//                JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));
//                jssc.checkpoint(checkPointDir);
//                Map<String, String> kafkaParams = new HashMap<String, String>();
//                kafkaParams.put(Constants.KAFKA_METADATA_BROKER_LIST, ConfigurationManager.getProperty(Constants.KAFKA_METADATA_BROKER_LIST));//放入kafka地址
//                //构建topic set
//                String kafkaTopics = ConfigurationManager.getProperty(Constants.KAFKA_TOPICS);
//                String[] kafkatopicsSplited = kafkaTopics.split(",");
//                final Set<String> topics = new HashSet<String>();
//                for (String kafkatopic : kafkatopicsSplited) {
//                    topics.add(kafkatopic);
//                }
//                //基于kafka direct api模式,构建出针对kafka集群中指定的topic的数据DStream
//                //两个值,val1,val2,val1没有什么意义,val2包含了kafka topic中的一条一条的实时日志数据
//                final JavaPairInputDStream<String, String> adRealTimeLogDStream = KafkaUtils.createDirectStream(
//                        jssc,
//                        String.class,
//                        String.class,
//                        StringDecoder.class,
//                        StringDecoder.class,
//                        kafkaParams,
//                        topics);
//                //TODO 过滤黑名单数据
//                JavaPairDStream<String, String> filteredADRealTimeLogDStream = filterByBlacklist(adRealTimeLogDStream);
//                //TODO 生成动态黑名单
//                generateDynamicBlacklist(filteredADRealTimeLogDStream);
//                //TODO 业务功能一: 计算广告点击流量实时统计结果(yyyyMMdd_provinve_city_adid,clickCount)
//                JavaPairDStream<String, Long> adRealTimeStatDStream = calculateRealTimeStat(filteredADRealTimeLogDStream);
//                //TODO 业务功能二: 实时统计每天每个省份top3热门广告
//                calculateProvinceTop3Ad(adRealTimeStatDStream);
//                //TODO 业务功能三: 实时统计每天每个广告在最近1小时的滑动窗口内的点击趋势(每分钟的点击量)
//                //我们每次都可以看到每个广告最近一小时内每分钟的点击量,每只广告的点击趋势
//                calculateAdClickByWindow(adRealTimeLogDStream);
//                //TODO 构建完spark streaming上下文之后,记得要进行上下文启动,等待执行结束,关闭
//                jssc.start();
//                jssc.awaitTermination();
//                jssc.close();
//                return jssc;
//            }
//        };
//
//        JavaStreamingContext context = JavaStreamingContext.getOrCreate(checkPointDir, contextFactory);
//        context.start();
//        context.awaitTermination();
//    };


    //TODO 根据黑名单过滤
    private static JavaPairDStream<String, String> filterByBlacklist(JavaPairInputDStream<String, String> adRealTimeLogDStream) {
        //刚刚接收到用户原始日志
        //根据mysql中的动态黑名单,进行实时黑名单过滤
        //使用transform算子(将dstream中的每个Batch RDD进行处理,转换成任意其他的RDD,功能强大)
        JavaPairDStream<String, String> filteredADRealTimeLogDStream = adRealTimeLogDStream.transformToPair(new Function<JavaPairRDD<String, String>, JavaPairRDD<String, String>>() {
            @Override
            public JavaPairRDD<String, String> call(JavaPairRDD<String, String> rdd) throws Exception {
                //首先从mysql中查询所有黑名单用户,将其转换为RDD
                IAdBlacklistDao adBlacklistDao = DAOFactory.getAdBlacklistDAO();
                List<AdBlacklist> adBlacklists = adBlacklistDao.findAll();

                final ArrayList<Tuple2<Long, Boolean>> tuples = new ArrayList<Tuple2<Long, Boolean>>();

                for (AdBlacklist adBlacklist : adBlacklists) {
                    tuples.add(new Tuple2<Long, Boolean>(adBlacklist.getUserid(), true));
                }

                JavaSparkContext sc = new JavaSparkContext(rdd.context());
                JavaPairRDD<Long, Boolean> blacklistRDD = sc.parallelizePairs(tuples);

                //将原始数据rdd映射成<userid,tuple2<Strng,String>>
                JavaPairRDD<Long, Tuple2<String, String>> mappedRDD = rdd.mapToPair(new PairFunction<Tuple2<String, String>, Long, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<Long, Tuple2<String, String>> call(Tuple2<String, String> tuple) throws Exception {
                        String log = tuple._2;
                        String[] logSplited = log.split(" ");
                        long userid = Long.valueOf(logSplited[3]);

                        return new Tuple2<Long, Tuple2<String, String>>(userid, tuple);
                    }
                });
                //将原始日志数据RDD与黑名单rdd,进行左外连接
                //如果原始日志的userid,没有对应的黑名单中,join不到,左外连接
                //用inner join,内连接,会导致数据丢失
                JavaPairRDD<Long, Tuple2<Tuple2<String, String>, com.google.common.base.Optional<Boolean>>> joinedRDD = mappedRDD.leftOuterJoin(blacklistRDD);
                JavaPairRDD<Long, Tuple2<Tuple2<String, String>, Optional<Boolean>>> filteredRDD = joinedRDD.filter(new Function<Tuple2<Long, Tuple2<Tuple2<String, String>, Optional<Boolean>>>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<Long, Tuple2<Tuple2<String, String>, Optional<Boolean>>> tuple) throws Exception {
                        Optional<Boolean> optional = tuple._2._2;
                        //如果这个值存在,那么原始日志join到了某个黑名单用户
                        if (optional.isPresent() && optional.get()) {
                            return false;
                        }
                        return true;
                    }
                });
                JavaPairRDD<String, String> resultRDD = filteredRDD.mapToPair(new PairFunction<Tuple2<Long, Tuple2<Tuple2<String, String>, Optional<Boolean>>>, String, String>() {
                    @Override
                    public Tuple2<String, String> call(Tuple2<Long, Tuple2<Tuple2<String, String>, Optional<Boolean>>> tuples) throws Exception {
                        return tuples._2._1;
                    }
                });
                return resultRDD;
            }
        });
        return filteredADRealTimeLogDStream;
    }

    //TODO 动态生成黑名单
    private static void generateDynamicBlacklist(JavaPairDStream<String, String> filteredADRealTimeLogDStream) {
        //一条一条实时日志
        //timestamp province city userid adid
        //某个时间点 某个省份 某个城市 某个用户 某个广告

        //计算出每个5秒内的数据中,每天每个用户每个广告的点击量

        //通过对原始实时日志的处理,将日志的格式处理成<yyyyMMdd_userid_add,1L>格式
        JavaPairDStream<String, Long> daliyUserAdClickDStream = filteredADRealTimeLogDStream.mapToPair(new PairFunction<Tuple2<String, String>, String, Long>() {
            @Override
            public Tuple2<String, Long> call(Tuple2<String, String> tuple) throws Exception {
                //从tupic中获取到每一条原始的实时日志
                String log = tuple._2;
                String[] logSplited = log.split(" ");
                //提取出日期(yyyyMMdd),userid,adid
                String timestamp = logSplited[0];
                Date date = new Date(Long.valueOf(timestamp));
                String datekey = DateUtils.formatDateKey(date);

                long userid = Long.valueOf(logSplited[3]);
                long adid = Long.valueOf(logSplited[4]);

                //拼接key
                String key = datekey + "_" + userid + "_" + adid;

                return new Tuple2<String, Long>(key, 1L);
            }
        });
        //针对处理后的格式,执行reduceByKey算子即可
        //(对每个Batch)每天每个用户对每个广告的点击量
        //TODO <yyyyMMdd_userid_adid,clickCount>
        JavaPairDStream<String, Long> dailyUserAdClickCountDStream = daliyUserAdClickDStream.reduceByKey(new Function2<Long, Long, Long>() {
            @Override
            public Long call(Long v1, Long v2) throws Exception {
                return v1 + v2;
            }
        });

        //到这里位置,获取到了什么数据呢?
        //dailyUserAdClickCountDStream DStream
        //TODO 源源不断的,每个5秒的batch中当天每个用户对每个广告的点击次数
        //<yyyyMMdd_userid_adid,clickCount>
        dailyUserAdClickCountDStream.foreachRDD(new Function<JavaPairRDD<String, Long>, Void>() {
            @Override
            public Void call(JavaPairRDD<String, Long> rdd) throws Exception {
                //对每个分区的数据尽量去创建一次连接对象
                //每次都是从连接池中获取,而不是每次都创建
                //写数据库操作,性能已经是提交到最高了

                rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Long>>>() {
                    @Override
                    public void call(Iterator<Tuple2<String, Long>> iterator) throws Exception {
                        List<AdUserClickCount> adUserClickCounts = new ArrayList<AdUserClickCount>();

                        while (iterator.hasNext()) {
                            Tuple2<String, Long> tuple = iterator.next();
                            String[] keySplited = tuple._1.split("_");

                            String date = DateUtils.formatDate(DateUtils.parseDateKey(keySplited[0]));
                            //yyyy-MM-dd
                            long userid = Long.valueOf(keySplited[1]);
                            long adid = Long.valueOf(keySplited[2]);
                            long clickCount = tuple._2;

                            AdUserClickCount adUserClickCount = new AdUserClickCount();
                            adUserClickCount.setDate(date);
                            adUserClickCount.setUserid(userid);
                            adUserClickCount.setAdid(adid);
                            adUserClickCount.setClickCount(clickCount);

                            adUserClickCounts.add(adUserClickCount);
                        }
                        IAdUserClickCountDAO adUserClickCountDAO = DAOFactory.getAdUserClickCountDAO();
                        adUserClickCountDAO.updateBatch(adUserClickCounts);
                    }
                });
                return null;
            }
        });
        //现在我们mysql中已经有了累计每天各用户对各广告的点击量
        //遍历每个batch中的所有记录,对每条记录都要去查询一下,这一天这个用户对这个广告的累计点击量是多少
        //从mysql中查询
        //查询出来的结果,如果是100,如果发现某个用户某天对某个广告的点击量已经大于100了
        //那么就判定这个用户是黑名单用户,就写入mysql表中去持久化

        //对batch中的数据,去查询mysql中的点击次数,使用哪个dstream
        //dailyUserAdClickCountDStream
        //为什么用这个batch,因为这个是聚合过的,已经按照yyyyMMdd聚合过了,
        //比如原始数据可能是一个batch有10000条,聚合过后可能有5000条,
        //所以选择聚合过的dstream,既可以满足我们的需求,而且,还能减少数据量
        //TODO 过滤出要加入黑名单用户
        JavaPairDStream<String, Long> blacklistDStream = dailyUserAdClickCountDStream.filter(new Function<Tuple2<String, Long>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, Long> tuple) throws Exception {
                String key = tuple._1;
                String[] keySplited = key.split("_");

                //yyyyMMdd -> yyyy-MM-dd
                String date = DateUtils.formatDate(DateUtils.parseDateKey(keySplited[0]));
                long userid = Long.valueOf(keySplited[1]);
                long adid = Long.valueOf(keySplited[2]);

                //从mysql中查询指定日期指定用户对指定广告的点击量
                IAdUserClickCountDAO adUserClickCountDAO = DAOFactory.getAdUserClickCountDAO();
                int clickCount = adUserClickCountDAO.findClickCountByMultKey(date, userid, adid);

                //判断,如果点击量大于等于100,那么,及时黑名单
                //拉入黑名单,返回true
                if (clickCount >= 100) {
                    return true;
                }
                //反之,如果点击量小于100,那么就暂时不管
                return false;
            }
        });

        //里面的每个batch,其实就是过滤出来的已经在某天对某个广告点击量超过100的用户
        //遍历这个dstream中的每个rdd,然后将黑名单用户增加到mysql中.
        //这里一旦增加以后,在整个程序里面,会加上黑名单过滤用户逻辑
        //所以,一旦用户被拉入黑名单以后,以后就不会出现在这里
        //所以直接插入mysql中
        //TODO 实际上,是要通过对dstream执行操作.对其中的dstream执行操作,对其中的rdd中的userid进行全局去重
        JavaDStream<Long> blacklistUseridDStream = blacklistDStream.map(new Function<Tuple2<String, Long>, Long>() {
            @Override
            public Long call(Tuple2<String, Long> tuple) throws Exception {
                String key = tuple._1;
                String[] keySplited = key.split("_");
                long userid = Long.valueOf(keySplited[1]);
                return userid;
            }
        });
        JavaDStream<Long> distinctBlacklistUseridDStream = blacklistUseridDStream.transform(new Function<JavaRDD<Long>, JavaRDD<Long>>() {
            @Override
            public JavaRDD<Long> call(JavaRDD<Long> rdd) throws Exception {
                return rdd.distinct();
            }
        });
        //到这一步为止distinctBlacklistUseridDStream
        //TODO 每一个rdd,只包含了userid,而且还进行了全局去重,保证每一次过滤出来的黑名单用户都没有重复的
        distinctBlacklistUseridDStream.foreachRDD(new Function<JavaRDD<Long>, Void>() {
            @Override
            public Void call(JavaRDD<Long> rdd) throws Exception {
                rdd.foreachPartition(new VoidFunction<Iterator<Long>>() {
                    @Override
                    public void call(Iterator<Long> iterator) throws Exception {
                        List<AdBlacklist> adBlacklists = new ArrayList<AdBlacklist>();
                        while (iterator.hasNext()) {
                            Long userid = iterator.next();
                            AdBlacklist adBlacklist = new AdBlacklist();
                            adBlacklist.setUserid(userid);
                            adBlacklists.add(adBlacklist);
                        }
                        IAdBlacklistDao adBlacklistDao = DAOFactory.getAdBlacklistDAO();
                        adBlacklistDao.insertBatch(adBlacklists);
                        //到此为止,实现了动态黑名单
                        //1 计算出每个batch中的每天每个用户对每个广告的点击量,并持久化到mysql中
                        //2 依据上述计算出来的数据,对每个batch中的按date,userid,adid聚合的数据
                        //都要遍历一遍,查询一遍,如果超过了100,那么就认定为黑名单用户,对黑名单用户去重,去重后,将黑名单用户,持久化到mysql中
                        //所以说mysql中ad_blacklist表中的黑名单用户,就是动态的湿湿的增长的
                        //所以说mysql中的ad_balcklist表,就是动态黑名单

                        //3 基于上述计算出来的动态黑名单,在一开始,就对每一个batch中的点击行为
                        //根据动态黑名单进行过滤
                        //把黑名单中的用户行为直接过滤掉
                        //动态黑名单机制完成了
                    }
                });
                return null;
            }
        });
    }

    /**
     * TODO 计算广告点击流量实时统计
     *
     * @param filteredADRealTimeLogDStream
     * @return
     */
    //上面的黑名单是广告类的实时系统中,比较常见的一种基础应用
    //计算每天每个省,各个城市广告的点击量
    //将j2ee系统,每隔几秒钟就从mysql取数据,
    //设计出来几个维度:日期,省份,城市,广告
    //用户可以看到,实时的数据,历史数据
    //date province city userid adid
    //date_province_city_adid 作为key;1作为value
    //通过spark直接统计出来全局的点击次数,在spark集群中保留一份,在mysql中也保留一份
    //对原始数据进行map,映射成<date_province_city_adid,1>
    //然后,对上述数据,执行updateStateByKey算子
    //执行updateStateByKey算子是sparkStreaming中特有的算子,在spark集群内存中,维护一份key的全局状态
    private static JavaPairDStream<String, Long> calculateRealTimeStat(JavaPairDStream<String, String> filteredADRealTimeLogDStream) {
        JavaPairDStream<String, Long> mappedDStream = filteredADRealTimeLogDStream.mapToPair(new PairFunction<Tuple2<String, String>, String, Long>() {
            @Override
            public Tuple2<String, Long> call(Tuple2<String, String> tuple) throws Exception {
                String log = tuple._2;
                String[] logSplited = log.split(" ");

                String timestamp = logSplited[0];
                Date date = new Date(Long.valueOf(timestamp));
                String datekey = DateUtils.formatDateKey(date);

                String province = logSplited[1];
                String city = logSplited[2];
                long adid = Long.valueOf(logSplited[4]);

                String key = datekey + "_" + province + "_" + adid;

                return new Tuple2<String, Long>(key, 1L);
            }
        });
        //TODO 在这个DStream中,就相当于,有每个batch rdd累加的各个key,(各天各省份各城市各广告的点击次数)
        //TODO 每次计算出最新的值,就在aggregateDStream中每个batch rdd中反映出来
        //TODO 然后,对上述数据,执行updateStateByKey算子
        //TODO 执行updateStateByKey算子是sparkStreaming中特有的算子,在spark集群内存中,维护一份key的全局状态
        JavaPairDStream<String, Long> aggregateDStream = mappedDStream.updateStateByKey(new Function2<List<Long>, Optional<Long>, Optional<Long>>() {
            @Override
            public Optional<Long> call(List<Long> values, Optional<Long> optional) throws Exception {
                long clickCount = 0L;
                //如果之前存在状态,就已之前的状态作为起点,进行值得累加
                if (optional.isPresent()) {
                    clickCount = optional.get();
                }
                //values 代表batch RDD中每个key对应的所有的值
                for (Long value : values) {
                    clickCount += value;
                }
                return Optional.of(clickCount);
            }
        });
        //将计算出来的最新结果,同步到一份mysql中,以便j2ee系统使用
        aggregateDStream.foreachRDD(new Function<JavaPairRDD<String, Long>, Void>() {
            @Override
            public Void call(JavaPairRDD<String, Long> rdd) throws Exception {
                rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Long>>>() {
                    @Override
                    public void call(Iterator<Tuple2<String, Long>> iterator) throws Exception {
                        List<AdStat> adStats = new ArrayList<AdStat>();
                        while (iterator.hasNext()) {
                            Tuple2<String, Long> tuple = iterator.next();
                            String[] keySplited = tuple._1.split("_");
                            String date = keySplited[0];
                            String province = keySplited[1];
                            String city = keySplited[2];
                            Long adid = Long.valueOf(keySplited[3]);

                            long clickCount = tuple._2;

                            AdStat adStat = new AdStat();
                            adStat.setDate(date);
                            adStat.setProvince(province);
                            adStat.setCity(city);
                            adStat.setAdid(adid);
                            adStat.setClickCount(clickCount);

                            adStats.add(adStat);
                        }
                        IAdStatDAO adStatDAO = DAOFactory.getAdStatDAO();
                        adStatDAO.updateBatch(adStats);
                    }
                });
                return null;
            }
        });
        return aggregateDStream;
    }


    /**
     * TODO 计算每天各省份的top3热门广告
     *
     * @param adRealTimeStatDStream
     */
    private static void calculateProvinceTop3Ad(JavaPairDStream<String, Long> adRealTimeStatDStream) {
        //adRealTimeStatDStream
        //每一个batch rdd都代表了最新的全量的每天各省份各城市各广告的点击量
        JavaDStream<Row> rowsDStream = adRealTimeStatDStream.transform(new Function<JavaPairRDD<String, Long>, JavaRDD<Row>>() {
            @Override
            public JavaRDD<Row> call(JavaPairRDD<String, Long> rdd) throws Exception {
                //<yyyyMMdd_provinve_city_adid,clickCount>
                //<yyyyMMdd_provinve_adid,clickCount>
                //计算出每天省份各广告的点击量
                JavaPairRDD<String, Long> mappedRDD = rdd.mapToPair(new PairFunction<Tuple2<String, Long>, String, Long>() {
                    @Override
                    public Tuple2<String, Long> call(Tuple2<String, Long> tuple) throws Exception {
                        String[] keySplited = tuple._1.split("_");
                        String date = keySplited[0];
                        String province = keySplited[1];
                        long adid = Long.valueOf(keySplited[3]);
                        long clickCount = tuple._2;

                        String key = date + "_" + province + "_" + adid;

                        return new Tuple2<String, Long>(key, clickCount);
                    }
                });
                JavaPairRDD<String, Long> aggrRDDAdClickCountByProvinceRDD = mappedRDD.reduceByKey(new Function2<Long, Long, Long>() {
                    @Override
                    public Long call(Long v1, Long v2) throws Exception {
                        return v1 + v2;
                    }
                });

                //将aggrRDDAdClickCountByProvinceRDD转换为DataFrame
                //注册一张临时表
                //使用spark SQL,通过开窗函数,获得各省份的top3热门广告
                JavaRDD<Row> rowsRDD = aggrRDDAdClickCountByProvinceRDD.map(new Function<Tuple2<String, Long>, Row>() {
                    @Override
                    public Row call(Tuple2<String, Long> tuple) throws Exception {
                        String[] keySplited = tuple._1.split("_");
                        String datekey = keySplited[0];
                        String provice = keySplited[1];
                        long adid = Long.valueOf(keySplited[2]);
                        long clickCount = tuple._2;

                        String date = DateUtils.formatDate(DateUtils.parseDateKey(datekey));

                        return RowFactory.create(date, provice, adid, clickCount);
                    }
                });

                StructType schema = DataTypes.createStructType(Arrays.asList(
                        DataTypes.createStructField("date", DataTypes.StringType, true),
                        DataTypes.createStructField("provice", DataTypes.StringType, true),
                        DataTypes.createStructField("ad_id", DataTypes.LongType, true),
                        DataTypes.createStructField("click_Count", DataTypes.LongType, true)
                ));

                HiveContext sqlContext = new HiveContext(rdd.context());
                DataFrame dailyAdClickCountByProvinceDF = sqlContext.createDataFrame(rowsRDD, schema);
                //将dailyAdClickCountByProvinceDF注册成一张临时表
                dailyAdClickCountByProvinceDF.registerTempTable("tmp_daily_ad_click_count_by_prov");
                //使用spark SQL 执行sql语句,配合开窗函数,统计出各省份top3热门广告
                DataFrame provinceTop3AdDF = sqlContext.sql(
                        "SELECT " +
                                "date," +
                                "province," +
                                "ad_id," +
                                "click_count," +
                                "FROM (" +
                                "SELECT " +
                                "date," +
                                "province," +
                                "ad_id," +
                                "click_count," +
                                "ROW_NUMBER() OVER(PARTITION BY province ORDER BY click_count DESC) rank" +
                                "FROM tmp_daily_ad_click_count_by_prov " +
                                ") t" +
                                "WHERE rank>3"
                );
                return provinceTop3AdDF.javaRDD();
            }
        });
        //每次都是刷新出来各个省份最热门的top3 广告
        //将其中的数据批量更新到mysql中
        rowsDStream.foreachRDD(new Function<JavaRDD<Row>, Void>() {
            @Override
            public Void call(JavaRDD<Row> rdd) throws Exception {
                rdd.foreachPartition(new VoidFunction<Iterator<Row>>(){
                    @Override
                    public void call(Iterator<Row> iterator) throws Exception {
                        ArrayList<AdProvinceTop3> adProvinceTop3s = new ArrayList<AdProvinceTop3>();
                        while (iterator.hasNext()){
                            Row row = iterator.next();
                            String date = row.getString(1);
                            String province = row.getString(2);
                            long adid = row.getLong(3);
                            long clickCount = row.getLong(4);

                            AdProvinceTop3 adProvinceTop3 = new AdProvinceTop3();
                            adProvinceTop3.setDate(date);
                            adProvinceTop3.setProvince(province);
                            adProvinceTop3.setAdid(adid);
                            adProvinceTop3.setClickCount(clickCount);

                            adProvinceTop3s.add(adProvinceTop3);
                        }
                        IAdProvinceTop3DAO adProvinceTop3DAO = DAOFactory.getAdProvinceTop3DAO();
                        adProvinceTop3DAO.updateBatch(adProvinceTop3s);
                    }
                });
                return null;
            }
        });
    }

    /**
     * TODO 计算最近1h滑动窗口内的广告点击趋势
     * @param adRealTimeLogDStream
     */
    private static void calculateAdClickByWindow(JavaPairInputDStream<String, String> adRealTimeLogDStream){
        //映射成<yyyyMMddHHmm_adid,1L>格式
        JavaPairDStream<String, Long> pairDStream = adRealTimeLogDStream.mapToPair(new PairFunction<Tuple2<String, String>, String, Long>() {
            @Override
            public Tuple2<String, Long> call(Tuple2<String, String> tuple) throws Exception {
                //timestamp province city userid adid
                String[] logSplited = tuple._2.split(" ");
                String timeMinute = DateUtils.formatTimeMinute(new Date(Long.valueOf(logSplited[0])));
                long adid = Long.valueOf(logSplited[4]);
                return new Tuple2<String, Long>(timeMinute+"_"+adid,1L);
            }
        });
        //过来的每个batch rdd, 都会被映射成<yyyyMMddHHMM_adid,1L>格式
        //每次出来一个新的batch,都要获取最近1h内的所有batch,
        //然后根据key进行reducebykey,统计出来,最近一小时内,的个分钟各广告的点击次数
        //1h内的滑动窗口的广告点击趋势
        //点图,折线图
        JavaPairDStream<String, Long> aggrRDD = pairDStream.reduceByKeyAndWindow(new Function2<Long, Long, Long>() {
            @Override
            public Long call(Long v1, Long v2) throws Exception {
                return v1 + v2;
            }
        }, Durations.minutes(60), Durations.seconds(10));
       // aggrRDD 每次都可以拿到,最近1h内,各分钟(yyyyMMddHHmm)各广告的的点击量
        //各广告,在最近1h内各分钟的点击量
        aggrRDD.foreachRDD(new Function<JavaPairRDD<String, Long>, Void>() {
            @Override
            public Void call(JavaPairRDD<String, Long> rdd) throws Exception {
                rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Long>>>(){
                    @Override
                    public void call(Iterator<Tuple2<String, Long>> iterator) throws Exception {
                        List<AdClickTrend> adClickTrends = new ArrayList<AdClickTrend>();
                        while(iterator.hasNext()){
                            Tuple2<String, Long> tuple = iterator.next();
                            //yyyyMMddHHmm
                            String[] keySplited = tuple._1.split("_");
                            String dateMinute = keySplited[0];
                            long adid = Long.valueOf(keySplited[1]);
                            long clickCount = tuple._2;

                            String date =DateUtils.formatDate(DateUtils.parseDateKey(dateMinute.substring(0,8)));
                            String hour = dateMinute.substring(8,10);
                            String minute = dateMinute.substring(10);

                            AdClickTrend adClickTrend = new AdClickTrend();
                            adClickTrend.setDate(date);
                            adClickTrend.setHour(hour);
                            adClickTrend.setMinute(minute);
                            adClickTrend.setAddid(adid);
                            adClickTrend.setClickCount(clickCount);

                            adClickTrends.add(adClickTrend);
                        }

                        IAdClickTrendDAO adClickTrendDAO = DAOFactory.getAdClickTrendDAO();
                        adClickTrendDAO.updateBatch(adClickTrends);
                    }
                });
                return null;
            }
        });
    }
}
