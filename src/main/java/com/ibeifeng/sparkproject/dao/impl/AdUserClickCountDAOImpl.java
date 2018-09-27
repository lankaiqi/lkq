package com.ibeifeng.sparkproject.dao.impl;

import com.ibeifeng.sparkproject.dao.IAdUserClickCountDAO;
import com.ibeifeng.sparkproject.domain.AdUserClickCount;
import com.ibeifeng.sparkproject.domain.AdUserClickCountQueryResult;
import com.ibeifeng.sparkproject.jdbc.JDBCHelper;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

/**
 * 用户广告点击量DAO实现类
 */
public class AdUserClickCountDAOImpl implements IAdUserClickCountDAO {

    /**
     * 批量更新用户广告点击量
     * @param adUserClickCounts
     */
    @Override
    public void updateBatch(List<AdUserClickCount> adUserClickCounts) {
        JDBCHelper jdbcHelper = JDBCHelper.getinstance();

        //首先对用户广告点击量进行分类,可插入,可更新
        List<AdUserClickCount> insertAdUserClickCount = new ArrayList<AdUserClickCount>();
        List<AdUserClickCount> updateAdUserClickCount = new ArrayList<AdUserClickCount>();

        String selectsql = "select count(*) from ad_user_click_count where date=? and user_id=? and ad_id=?";

        Object[] selectParams = null;

        for (AdUserClickCount adUserClickCount:adUserClickCounts){
            final AdUserClickCountQueryResult queryResult = new AdUserClickCountQueryResult();
            selectParams = new Object[]{
                    adUserClickCount.getDate(),
                    adUserClickCount.getUserid(),
                    adUserClickCount.getAdid()
            };

            jdbcHelper.executeQuery(selectsql, selectParams, new JDBCHelper.QueryCallback() {
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
                updateAdUserClickCount.add(adUserClickCount);
            }else {
                insertAdUserClickCount.add(adUserClickCount);
            }
        }
        //执行批量插入
        String insertSQL = "insert into ad_user_click_count values(?,?,?,?)";
        List<Object[]> insertParamsList = new ArrayList<Object[]>();

        for (AdUserClickCount adUserClickCount :insertAdUserClickCount){
            Object[] insertParam = new Object[]{
                    adUserClickCount.getDate(),
                    adUserClickCount.getUserid(),
                    adUserClickCount.getAdid(),
                    adUserClickCount.getClickCount()};
            insertParamsList.add(insertParam);
        }

        jdbcHelper.executBatch(insertSQL,insertParamsList);

        //执行批量更新
        //
        String updateSQL = "update ad_user_click_count SET click_count=? where date=? and user_id=? and ad_id=?";
        List<Object[]> updateParamsList = new ArrayList<Object[]>();

        for (AdUserClickCount adUserClickCount :updateAdUserClickCount){
            Object[] insertParam = new Object[]{
                    adUserClickCount.getClickCount(),
                    adUserClickCount.getDate(),
                    adUserClickCount.getUserid(),
                    adUserClickCount.getAdid()
                   };
            insertParamsList.add(insertParam);
        }

        jdbcHelper.executBatch(updateSQL,updateParamsList);
    }

    /**
     * 根据多个key查询用户点击量
     * @param date  日期
     * @param userid  用户id
     * @param adid  广告id
     * @return
     */
    @Override
    public int findClickCountByMultKey(String date, long userid, long adid) {
        String sql ="select click_count from ad_user_click_count where date =? and user_id=? and ad_id=?";

        Object[] params = new Object[] {date,userid,adid};

        final AdUserClickCountQueryResult queryResult = new AdUserClickCountQueryResult();

        JDBCHelper jdbcHelper = JDBCHelper.getinstance();
        jdbcHelper.executeQuery(sql,params,new JDBCHelper.QueryCallback(){
            @Override
            public void process(ResultSet rs) throws Exception {
                if (rs.next()){
                    int clickCount = rs.getInt(1);
                    queryResult.setClickCount(clickCount);
                }
            }
        });
        int clickCount = queryResult.getClickCount();
        return clickCount;
    }

}
