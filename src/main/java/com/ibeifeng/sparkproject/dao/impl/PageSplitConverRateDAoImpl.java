package com.ibeifeng.sparkproject.dao.impl;

import com.ibeifeng.sparkproject.dao.IPageSplitConverRateDAo;
import com.ibeifeng.sparkproject.domain.PageSplitConvertRate;
import com.ibeifeng.sparkproject.jdbc.JDBCHelper;

/**
 *页面切片转化率DAO实现类
 */
public class PageSplitConverRateDAoImpl implements IPageSplitConverRateDAo {
    @Override
    public void insert(PageSplitConvertRate pageSplitConvertRate) {
        String sql = "insert into page_split_conver_rate values(?,?)";
        Object[] params = new Object[]{
          pageSplitConvertRate.getTaskid(),
          pageSplitConvertRate.getConverRate()
        };
        JDBCHelper jdbcHelper = JDBCHelper.getinstance();
        jdbcHelper.executeUpdate(sql,params);
    }
}
