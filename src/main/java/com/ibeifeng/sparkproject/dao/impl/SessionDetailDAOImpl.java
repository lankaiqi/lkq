package com.ibeifeng.sparkproject.dao.impl;

import com.ibeifeng.sparkproject.dao.ISessionDetailDAO;
import com.ibeifeng.sparkproject.domain.SessionDetail;
import com.ibeifeng.sparkproject.jdbc.JDBCHelper;

import java.util.ArrayList;
import java.util.List;

/**
 * session明细DAO实现类
 */
public class SessionDetailDAOImpl implements ISessionDetailDAO{

    /**
     * 插入一条session明细数据
     * @param sessionDetail
     */
    public void insert(SessionDetail sessionDetail) {
        String sql = "insert into session_detail values(?,?,?,?,?,?,?,?,?,?,?,?)";
        Object[] params = new Object[]{
                sessionDetail.getTaskid(),
                sessionDetail.getUserid(),
                sessionDetail.getSessionid(),
                sessionDetail.getPageid(),
                sessionDetail.getActionTime(),
                sessionDetail.getSearchKeyword(),
                sessionDetail.getClickCategoryId(),
                sessionDetail.getClickProductId(),
                sessionDetail.getOrderCategoryIds(),
                sessionDetail.getOrderProductIds(),
                sessionDetail.getPayCategoryIds(),
                sessionDetail.getPayProductIds()
        };

        JDBCHelper jdbchelper = JDBCHelper.getinstance();
        jdbchelper.executeUpdate(sql,params);
    }

    /**
     * 批量插入session明细数据
     * @param sessionDetails
     */
    @Override
    public void insertBatch(List<SessionDetail> sessionDetails) {
        String sql = "insert into session_detail values(?,?,?,?,?,?,?,?,?,?,?,?)";
        List<Object[]> paramList = new ArrayList<Object[]>();
        for (SessionDetail sessionDetail : sessionDetails){
            Object[] params = new Object[]{
                    sessionDetail.getTaskid(),
                    sessionDetail.getUserid(),
                    sessionDetail.getSessionid(),
                    sessionDetail.getPageid(),
                    sessionDetail.getActionTime(),
                    sessionDetail.getSearchKeyword(),
                    sessionDetail.getClickCategoryId(),
                    sessionDetail.getClickProductId(),
                    sessionDetail.getOrderCategoryIds(),
                    sessionDetail.getOrderProductIds(),
                    sessionDetail.getPayCategoryIds(),
                    sessionDetail.getPayProductIds()
            };
            paramList.add(params);
        };
        JDBCHelper jdbchelper = JDBCHelper.getinstance();
        jdbchelper.executBatch(sql,paramList);

    }
}