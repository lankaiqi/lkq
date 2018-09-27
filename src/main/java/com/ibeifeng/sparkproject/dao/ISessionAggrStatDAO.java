package com.ibeifeng.sparkproject.dao;

import com.ibeifeng.sparkproject.domain.SessionAggrStat;

/**
 * Created by Administrator on 2018/7/19.
 */
public interface ISessionAggrStatDAO {

    /**
     * 插入session聚合统计结果
     * @param sessionAggrStat
     */
    void insert (SessionAggrStat sessionAggrStat);

}
