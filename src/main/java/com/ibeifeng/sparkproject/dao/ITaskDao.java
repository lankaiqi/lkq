package com.ibeifeng.sparkproject.dao;

import com.ibeifeng.sparkproject.domain.Task;

/**
 * 任务管理DAO接口
 */
public interface ITaskDao {

    /**
     * 根据主键查询任务
     * @param taskid
     * @return
     */
    Task findById (long taskid);

}
