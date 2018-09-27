package com.ibeifeng.sparkproject.spark.page;

import com.alibaba.fastjson.JSONObject;
import com.ibeifeng.sparkproject.constant.Constants;
import com.ibeifeng.sparkproject.dao.IPageSplitConverRateDAo;
import com.ibeifeng.sparkproject.dao.ITaskDao;
import com.ibeifeng.sparkproject.dao.factory.DAOFactory;
import com.ibeifeng.sparkproject.domain.PageSplitConvertRate;
import com.ibeifeng.sparkproject.domain.Task;
import com.ibeifeng.sparkproject.util.DateUtils;
import com.ibeifeng.sparkproject.util.NumberUtils;
import com.ibeifeng.sparkproject.util.ParamUtils;
import com.ibeifeng.sparkproject.util.SparkUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import scala.Tuple2;

import java.util.*;

/**
 * 页面单挑转化率模块spark作业
 */
public class PageOneStepConverSpark {

    public static void main (String[] args){
        //1 构造上下文
        SparkConf conf = new SparkConf().setAppName(Constants.SPARK_APP_NAME_PAGE);
        SparkUtils.setMaster(conf);

        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = SparkUtils.getSQLContext(sc.sc());

        //2 生成模拟数据
        SparkUtils.mockData(sc,sqlContext);

        //3 查询任务,获取任务参数
        Long taskid = ParamUtils.getTaskIdFromArgs(args,Constants.SPARK_LOCAL_TASKID_PAGE);
        ITaskDao taskDAO = DAOFactory.getTaskDao();
        Task task = taskDAO.findById(taskid);
        if (task == null ){
            System.out.println(new Date() +":connot find this task with id {"+taskid+"}");
            return;
        }
        JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());

        //4 查询指定日期范围内的用户行为数据
        JavaRDD<Row> actionRDD = SparkUtils.getActionRDDByDateRange(sqlContext, taskParam);//TODO Row

        //对用户访问行为数据做一个映射,将其映射为<session,访问行为>的格式
        //咱们的用户访问页面切片的生成,是要基于每个session的访问数据,来进行生成的
        //脱离了session,生成的页面访问切片,是没有意义的
        //举例,用户A访问页面3,页面5
        //用户B,访问了,页面4 6
        //漏一个条件,筛选条件页面3->页面4->页面7
        // 将页面3->页面4串起来,作为一个页面切片,来进行统计,是不可以的
        //页面切片的生成,肯定是基于用户session粒度的
        //TODO 获取sessionid2到访问行为数据的映射的RDD  <sessionid,Row>
        JavaPairRDD<String, Row> sessionid2ActionRDD = getSessionid2ActionRDD(actionRDD);

        //TODO 对<sessionid,用户访问行为>RDD,做一次groupByKey操作
        //因为我们要拿到每个session对应的访问行为数据,才能够去生成切片
        JavaPairRDD<String, Iterable<Row>> sessionid2actionRDD = sessionid2ActionRDD.groupByKey();
        sessionid2actionRDD = sessionid2actionRDD.cache();// == persist(StorageLevel.MEMORY_ONLY())

        //TODO 最核心的一步,每个session的单跳页面切片的生成,以及页面流的匹配,算法
        JavaPairRDD<String,Integer>  pageSplitRDD = generateAndMatchPageSplit(sc,sessionid2actionRDD,taskParam);    //<2-3,1>
        Map<String,Object> pageSplitPvMap = pageSplitRDD.countByKey();      //<2-3,8>

        //使用者指定的页面3,2,5,8,6
        //咱们拿到的pageSplitPvMap 3->2,2->5,5->8,8->6
        //TODO 初始pv
        long startPagePv = getStartPagePv(taskParam, sessionid2actionRDD);

        //TODO 计算页面流的各个页面切片的转化率
        Map<String, Double> converRateMap = computePageSplitConverRate(taskParam, pageSplitPvMap, startPagePv);

        //TODO 持久化页面切片转化率
        persistConverRate(taskid,converRateMap);

        sc.close();
     }








    /**
     * TODO 获取<sessionid,用户访问行为>格式的数据
     * @param actionRDD 用户访问行为RDD
     * @return <sessionid,用户访问行为>格式的数据
     */
    private static JavaPairRDD<String,Row> getSessionid2ActionRDD(JavaRDD<Row> actionRDD){
        return actionRDD.mapToPair(new PairFunction<Row, String,Row>(){
            public Tuple2<String,Row> call(Row row) throws Exception {
                return new Tuple2<String,Row>(row.getString(2),row);
            }
        });
    }

    /**
     * TODO 页面生成与匹配算法
     * @param sc
     * @param sessionid2actionRDD
     * @param taskParam
     * @return
     */
    private static JavaPairRDD<String , Integer> generateAndMatchPageSplit( JavaSparkContext sc, JavaPairRDD<String, Iterable<Row>> sessionid2actionRDD ,  JSONObject taskParam){
        final String targetPageFlow = ParamUtils.getParam(taskParam, Constants.PARAM_TARGET_PAGE_FLOW);
        final Broadcast<String>  targetPageFlowBroadcast = sc.broadcast(targetPageFlow);

        return sessionid2actionRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Iterable<Row>>, String, Integer>(){
            public Iterable<Tuple2<String, Integer>> call(Tuple2<String, Iterable<Row>> tuple) throws Exception {
                //   定义返回list
                List<Tuple2<String,Integer>> list = new ArrayList<Tuple2<String,Integer>>();
                //获取到当前session访问行为的迭代器
                Iterator<Row> iterator = tuple._2.iterator();
                //获取指定者使用的页面流
                //使用者指定的页面流1,2,3,4,5,6,7
                //1->2转化率  2->3转化率
                String[] targetPages = targetPageFlowBroadcast.value().split(",");
                //这里session访问行为,是乱序的
                //我们希望拿到时间吮吸排序的
                //所以,第一件事是按照时间顺序排序

                //举例,反例 真实的3->5->4->7->10
                //拿到的是2->4->5->6->7->10

                List<Row> rows = new ArrayList<Row>();
                while(iterator.hasNext()){
                    rows.add(iterator.next());
                }
                //TODO 匿名内部类排序
                Collections.sort(rows, new Comparator<Row>() {
                    public int compare(Row o1, Row o2) {
                        String actionTime1 = o1.getString(4);
                        String actionTime2 = o2.getString(4);

                        Date date1 = DateUtils.parseTime(actionTime1);
                        Date date2 = DateUtils.parseTime(actionTime2);

                        return (int)(date1.getTime()-date2.getTime());
                    }
                });
                //页面切片的生成,以及页面流的匹配
                Long lastPageId = null;

                for (Row row:rows){
                    long pageid = row.getLong(3);
                    if (lastPageId == null){
                        lastPageId=pageid;
                        continue;
                    }
                    //生成一个页面切片
                    //3,5,2,1,8,9
                    //lastPageId=3   3_5
                    String pageSplit = lastPageId+"_"+pageid;

                    //对这个切片判断一下,是否在用户指定的页面流中
                    for(int i =1 ; i<targetPages.length ; i++){
                        //比如,用户指定的页面流 3,2,5,8,1
                        //遍历的时候从索引1开始,从第二个页面开始
                        //3_2切片,2_5切片
                        String targePageSplit = targetPages[i-1]+"_"+targetPages[i];

                        if (pageSplit.equals(targePageSplit)){
                            list.add(new Tuple2<String, Integer>(pageSplit,1));
                            break;
                        }
                    }
                    lastPageId=pageid;
                }
                return list;
            }
        });
    }

    /**
     * 获取页面流中初始页面的pv
     * @param taskParam
     * @param sessionid2actionRDD
     * @return
     */
    private static long getStartPagePv(JSONObject taskParam, JavaPairRDD<String, Iterable<Row>> sessionid2actionRDD  ){
        String targePageFlow = ParamUtils.getParam(taskParam,Constants.PARAM_TARGET_PAGE_FLOW);//定义的参数
        final long startPageId = Long.valueOf(targePageFlow.split(",")[0]);

        JavaRDD<Long> startPageRDD = sessionid2actionRDD.flatMap(new FlatMapFunction<Tuple2<String, Iterable<Row>>, Long>() {
            public Iterable<Long> call(Tuple2<String, Iterable<Row>> tuple) throws Exception {
                List<Long> list = new ArrayList<Long>();

                Iterator<Row> iterator = tuple._2.iterator();
                while (iterator.hasNext()) {
                    Row row = iterator.next();
                    long pageid = row.getLong(3);
                    if (pageid == startPageId) {
                    }
                    {
                        list.add(pageid);
                    }
                }
                return list;
            }
        });
        return sessionid2actionRDD.count();
    }

    /**
     * 计算页面切片转换率
     * @param pageSplitPvMap 页面切片pv
     * @param startPagePv   起始页面pv
     * @return
     */
    private static Map<String,Double> computePageSplitConverRate(JSONObject taskParam , Map<String,Object> pageSplitPvMap , long startPagePv){
        Map<String,Double> converRateMap = new HashMap<String, Double>();
        String aa = ParamUtils.getParam(taskParam, Constants.PARAM_TARGET_PAGE_FLOW);
        String[] targetPages = ParamUtils.getParam(taskParam, Constants.PARAM_TARGET_PAGE_FLOW).split(",");
        long lastPageSplitPv = 0L;
        //3,5,2,4,6
        //3_5
        //3_5pv/3pv
        //5_2pv/3_5pv

        //通过for循环获取目标页面流的各个切片(pv)
        for (int i=1 ;i<targetPages.length ;i++){
            String targetPageSplit = targetPages[i-1]+"_"+targetPages[i];
            String  sl = String.valueOf(pageSplitPvMap.get(targetPageSplit));
            long targetPageSplitPv=0L;
            if (sl==null){
                targetPageSplitPv=0L;
            }else if(sl.equals("null")){
                targetPageSplitPv=0L;
            }else {
                targetPageSplitPv  = Long.valueOf(sl);
            }
            double converRate = 0.0;
            if (i==1){
                // converRate = NumberUtils.formatDouble((double)targetPageSplitPv/(double)startPagePv,2);
                Double dd = (double)targetPageSplitPv/(double)startPagePv;
                if (dd==0) converRate=dd;
                else converRate = NumberUtils.formatDouble(dd,2);
            }else {
                //  converRate = NumberUtils.formatDouble((double)targetPageSplitPv/(double)lastPageSplitPv,2);
                Double dd = (double)targetPageSplitPv/(double)lastPageSplitPv;
                if (dd==0) converRate=dd;
                else converRate = NumberUtils.formatDouble(dd,2);
            }
            converRateMap.put(targetPageSplit,converRate);
            lastPageSplitPv = targetPageSplitPv;
        }
        return converRateMap;
    }

    /**
     * 持久化转化率
     * @param converRateMap
     * @param taskid
     */
    private static void persistConverRate(long taskid,Map<String,Double> converRateMap){
        StringBuffer buffer = new StringBuffer("");

        for (Map.Entry<String,Double> converRateEntry : converRateMap.entrySet()){
            String pageSplit = converRateEntry.getKey();
            double converRate = converRateEntry.getValue();
            buffer.append(pageSplit+"="+converRate+"|");
        }
        String converRate = buffer.toString();
        converRate = converRate.substring(0,converRate.length()-1);
        PageSplitConvertRate pageSplitConvertRate = new PageSplitConvertRate();
        pageSplitConvertRate.setTaskid(taskid);
        pageSplitConvertRate.setConverRate(converRate);
        IPageSplitConverRateDAo splitConverRateDAo = DAOFactory.getSplitConverRateDAo();
        splitConverRateDAo.insert(pageSplitConvertRate);
    }
}
