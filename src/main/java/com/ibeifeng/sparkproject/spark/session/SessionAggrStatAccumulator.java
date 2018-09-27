package com.ibeifeng.sparkproject.spark.session;

import com.ibeifeng.sparkproject.constant.Constants;
import com.ibeifeng.sparkproject.util.StringUtils;
import org.apache.spark.AccumulatorParam;

/**
 * session 聚合统计Accumulator
 * 其实使用自己定义的一些数据格式，比如String，甚至说，我们可以自己定义model，自己定义的类（必须可序列化）
 * 可以基于这种特殊的数据格式,可以实现复杂的分布式的计算逻辑
 * 各个task,分布式在运行,可以根据你的需求,task给Accumulator传入不同的值
 * 根据不同的值,去做复杂的逻辑
 *
 * Spark Core里面很实用的高端技术
 */
public class SessionAggrStatAccumulator implements AccumulatorParam<String>{

    /**
     * zero用于数据初始化
     * 那么我们返回一个值,就是初始化中,所有范围区间的数量都是0
     * 各个范围统计数量拼接 ,还是采用 key=value|key=value的连接串方式
     * @param initialValue
     * @return
     */
    @Override
    public String zero(String initialValue) {
        return Constants.SESSION_COUNT + "=0|"
                + Constants.TIME_PERIOD_1s_3s + "=0|"
                + Constants.TIME_PERIOD_4s_6s + "=0|"
                + Constants.TIME_PERIOD_7s_9s + "=0|"
                + Constants.TIME_PERIOD_10s_30s + "=0|"
                + Constants.TIME_PERIOD_30s_60s + "=0|"
                + Constants.TIME_PERIOD_1m_3m + "=0|"
                + Constants.TIME_PERIOD_3m_10m + "=0|"
                + Constants.TIME_PERIOD_10m_30m + "=0|"
                + Constants.TIME_PERIOD_30m + "=0|"
                + Constants.STEP_PERIOD_1_3 + "=0|"
                + Constants.STEP_PERIOD_4_6 + "=0|"
                + Constants.STEP_PERIOD_7_9 + "=0|"
                + Constants.STEP_PERIOD_10_30 + "=0|"
                + Constants.STEP_PERIOD_30_60 + "=0|"
                + Constants.STEP_PERIOD_60 + "=0";
    }

    /**
     * addInPlace 与 addAccumulator 可以理解为一样的
     *
     * 这两个方法主要是实现,v1 可能就是我们初始化的连接串,
     * v2 就是我们在遍历session的时候,判断出某个session对应的区间,然后会用Constants.TIME_PERIOD_1s_3s
     * 所以要做的是,在v1中找到v2,累加1,更新回连接串
     * @param v1
     * @param v2
     * @return
     */
    @Override
    public String addInPlace(String v1, String v2) {

        return add(v1,v2);
    }

    @Override
    public String addAccumulator(String v1, String v2) {

        return add(v1,v2);
    }

    private String add(String v1,String v2){
        //检验:v1为空的话,直接返回v2
        if(StringUtils.isEmpty(v1)){
            return v2;
        }
        //使用StringUtils工具类,从v1中提取v2对应的值,并累加1
        String oldValue = StringUtils.getFieldFromConcatString(v1, "\\|", v2);
        if (oldValue != null){
            //将范围区间原有值累加1
            int newValue = Integer.valueOf(oldValue)+1;
            //使用StringUtles工具类,将v1中,v2对应的值,设置成新的累加后的值
            return StringUtils.setFieldInConcatString(v1,"\\|",v2,String.valueOf(newValue));
        }
        return v1;
    }
}
