package com.ibeifeng.sparkproject.dao.impl;

import com.ibeifeng.sparkproject.dao.ITop10SessionDAO;
import com.ibeifeng.sparkproject.domain.Top10Session;
import com.ibeifeng.sparkproject.jdbc.JDBCHelper;

/**
 * Created by Administrator on 2018/7/23.
 */
public class Top10SessionDAOImpl implements ITop10SessionDAO {
    @Override
    public void insert(Top10Session Top10Session) {

        String sql = "insert into top10_session values(?,?,?,?)";

        Object[] params = new Object[]{
                Top10Session.getTaskid(),
                Top10Session.getCategoryid(),
                Top10Session.getSessionid(),
                Top10Session.getClickCount()
        };
        JDBCHelper jdbcHelper = JDBCHelper.getinstance();
        jdbcHelper.executeUpdate(sql, params);
    }
}
