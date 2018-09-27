package com.ibeifeng.sparkproject.dao.impl;

import com.ibeifeng.sparkproject.dao.IAdClickTrendDAO;
import com.ibeifeng.sparkproject.dao.factory.DAOFactory;
import com.ibeifeng.sparkproject.domain.AdClickTrend;
import com.ibeifeng.sparkproject.domain.AdStatQueryResult;
import com.ibeifeng.sparkproject.jdbc.JDBCHelper;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

/**
 * 广告点击趋势DAO实现类
 */
public class AdClickTrendDAOImpl implements IAdClickTrendDAO {
    @Override
    public void updateBatch(List<AdClickTrend> adClickTrends) {
        JDBCHelper jdbcHelper = JDBCHelper.getinstance();

       //区分出来插入与更新数据
        //提醒,通常来说,同一个key的数据(rdd,包含多条key)
        //通常是在一个分区内,不会出现重复插入
        List<AdClickTrend> updateAdClickTrends = new ArrayList<AdClickTrend>();
        List<AdClickTrend> insertAdClickTrends = new ArrayList<AdClickTrend>();

        String selectSQL ="SELECT count(*) " +
                "from ad_click_trend " +
                "where date=?  " +
                "and hour =? " +
                "and minute=? " +
                "and ad_id=?";
        for (AdClickTrend adClickTrend : adClickTrends){
            final AdStatQueryResult queryResult = new AdStatQueryResult();

            Object[] params = new Object[]{
                    adClickTrend.getDate(),
                    adClickTrend.getHour(),
                    adClickTrend.getMinute(),
                    adClickTrend.getAddid()
            };

            jdbcHelper.executeQuery(selectSQL,params, new JDBCHelper.QueryCallback() {
                @Override
                public void process(ResultSet rs) throws Exception {
                    if (rs.next()){
                        int count = rs.getInt(1);
                        queryResult.setCount(count);
                    }
                }
            });
            int count = queryResult.getCount();
            if (count>0){
                updateAdClickTrends.add(adClickTrend);
            }else {
                insertAdClickTrends.add(adClickTrend);
            }
        }
        //批量更新操作
        String updateSQL = "UPDATE ad_click_trend SET click_count=? " +
                "WHERE date=? " +
                "and hour =? " +
                "and minute=? " +
                "and ad_id=?";
        List<Object[]> updateParamsList = new ArrayList<Object[]>();
        for (AdClickTrend adClickTrend:updateAdClickTrends){
            Object[] params = new Object[]{
                    adClickTrend.getClickCount(),
                    adClickTrend.getDate(),
                    adClickTrend.getHour(),
                    adClickTrend.getMinute(),
                    adClickTrend.getAddid()
            };
            updateParamsList.add(params);
        }
       jdbcHelper.executBatch(updateSQL,updateParamsList);

        //批量插入
        String insertSQL = "INSERT INTO ad_click_trend values (?,?,?,?,?)";
        List<Object[]> insertParamsList = new ArrayList<Object[]>();
        for (AdClickTrend adClickTrend:insertAdClickTrends){
            Object[] params = new Object[]{
                    adClickTrend.getClickCount(),
                    adClickTrend.getDate(),
                    adClickTrend.getHour(),
                    adClickTrend.getMinute(),
                    adClickTrend.getAddid()
            };
            insertParamsList.add(params);
        }
        jdbcHelper.executBatch(insertSQL,insertParamsList);

    }
}
