package com.ibeifeng.sparkproject.dao.impl;

import com.ibeifeng.sparkproject.dao.IAdStatDAO;
import com.ibeifeng.sparkproject.domain.AdStat;
import com.ibeifeng.sparkproject.domain.AdStatQueryResult;
import com.ibeifeng.sparkproject.jdbc.JDBCHelper;
import org.apache.avro.generic.GenericData;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

/**
 *广告实时统计DAO实现类
 */
public class AdStatDAOImpl implements IAdStatDAO {
    @Override
    public void updateBatch(List<AdStat> adStatss) {
        JDBCHelper jdbcHelper = JDBCHelper.getinstance();
        List<AdStat> insertAdStats = new ArrayList<AdStat>();
        List<AdStat> updateAdStats = new ArrayList<AdStat>();

        String selectSql = "select count(*) from ad_stat where date=? and province =? and city=? and ad_id=?";

        for (AdStat adStat : adStatss){
            final AdStatQueryResult queryResult = new AdStatQueryResult();
            Object[] param = new Object[]{adStat.getDate(),
                    adStat.getProvince(),
                    adStat.getCity(),
                    adStat.getAdid()};
            jdbcHelper.executeQuery(selectSql, param, new JDBCHelper.QueryCallback() {
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
                updateAdStats.add(adStat);
            }else {
                insertAdStats.add(adStat);
            }
        }

        //对于需要插入的数据,批量插入
        String insertsql ="insert into ad_stat values(?,?,?,?,?)";

        List<Object[]> insertparamsList = new ArrayList<Object[]>();

        for (AdStat adStat : insertAdStats){
            Object[] param = new Object[]{adStat.getDate(),
                    adStat.getProvince(),
                    adStat.getCity(),
                    adStat.getAdid(),
                    adStat.getClickCount()};
            insertparamsList.add(param);
        }
        jdbcHelper.executBatch(insertsql,insertparamsList);

        //对于需要更新的数据,批量更新
        String updatesql ="update ad_stat set click_count=? from ad_stat where date=? and province =? and city=? and ad_id=?";

        List<Object[]> updateparamsList = new ArrayList<Object[]>();

        for (AdStat adStat : updateAdStats){
            Object[] param = new Object[]{ adStat.getClickCount(),
                    adStat.getDate(),
                    adStat.getProvince(),
                    adStat.getCity(),
                    adStat.getAdid()};
            updateparamsList.add(param);
        }
        jdbcHelper.executBatch(updatesql,updateparamsList);
    }
}
