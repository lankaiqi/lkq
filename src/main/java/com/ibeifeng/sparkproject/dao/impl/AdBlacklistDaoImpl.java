package com.ibeifeng.sparkproject.dao.impl;

import com.ibeifeng.sparkproject.dao.IAdBlacklistDao;
import com.ibeifeng.sparkproject.domain.AdBlacklist;
import com.ibeifeng.sparkproject.jdbc.JDBCHelper;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

/**
 * 广告黑名单DAO实现类
 */
public class AdBlacklistDaoImpl implements IAdBlacklistDao {
    @Override
    public void insertBatch(List<AdBlacklist> adBlacklistLists) {
        String sql = "insert into ad_blacklist values(?)";

        List<Object[]> paramsList = new ArrayList<Object[]>();

        for (AdBlacklist adBlacklistList:adBlacklistLists){
           Object[] params = new Object[]{adBlacklistList.getUserid()};
           paramsList.add(params);
        }
        JDBCHelper jdbcHelper = JDBCHelper.getinstance();
        jdbcHelper.executBatch(sql,paramsList);
;    }

    /**
     * 查询所有广告黑名单用户
     * @return
     */
    @Override
    public List<AdBlacklist> findAll() {
        String sql ="select * from ad_blacklist";

        final List<AdBlacklist> adBlacklists = new ArrayList<AdBlacklist>();

        JDBCHelper jdbcHelper = JDBCHelper.getinstance();

        jdbcHelper.executeQuery(sql, null, new JDBCHelper.QueryCallback() {
            @Override
            public void process(ResultSet rs) throws Exception {
                while(rs.next()){
                    Long userid = Long.valueOf(String.valueOf(rs.getInt(1)));
                    AdBlacklist adBlacklist = new AdBlacklist();
                    adBlacklist.setUserid(userid);
                    adBlacklists.add(adBlacklist);
                }
            }
        });
        return adBlacklists;
    }
}
