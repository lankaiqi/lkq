package com.ibeifeng.sparkproject.dao.impl;

import com.ibeifeng.sparkproject.dao.ITop10CategoryDAO;
import com.ibeifeng.sparkproject.domain.Top10Category;
import com.ibeifeng.sparkproject.jdbc.JDBCHelper;

/**
 * top10品类DAO实现
 */
public class Top10CategoryImpl implements ITop10CategoryDAO {
    public void insert(Top10Category top10Category) {
        String sql ="insert into top10_category values(?,?,?,?,?)";
        Object[] params = new Object[]{
                top10Category.getTaskid(),
                top10Category.getCategoryid(),
                top10Category.getClickCount(),
                top10Category.getOrderCount(),
                top10Category.getPayCount()
        };
        JDBCHelper jdbcHelper = JDBCHelper.getinstance();
        jdbcHelper.executeUpdate(sql,params);
    }
}
