package com.ibeifeng.sparkproject.dao.impl;

import com.ibeifeng.sparkproject.dao.ITaskDao;
import com.ibeifeng.sparkproject.domain.Task;
import com.ibeifeng.sparkproject.jdbc.JDBCHelper;

import java.sql.ResultSet;

/**
 * Created by Administrator on 2018/7/16.
 */
public class TaskDaoImpl implements ITaskDao{

    /**
     * 根据主键查询任务
     * @param taskid 主键
     * @return 任务
     */
    public Task findById(long taskid) {
        final Task task = new Task();
        String sql = "select * from task where task_id=?";
        Object[] params = new Object[]{taskid};
        JDBCHelper jdbcHelper = JDBCHelper.getinstance();
        jdbcHelper.executeQuery(sql,params, new JDBCHelper.QueryCallback() {

            @Override
            public void process(ResultSet rs) throws Exception {
                if (rs.next()){
                    long taskid = rs.getLong(1);
                    String taskName = rs.getString(2);
                    String creatTime = rs.getString(3);
                    String startTime = rs.getString(4);
                    String finishTime = rs.getString(5);
                    String taskType = rs.getString(6);
                    String taskStatus = rs.getString(7);
                    String taskParam = rs.getString(8);

                    task.setTaskid(taskid);
                    task.setTaskName(taskName);
                    task.setCreateTime(creatTime);
                    task.setStartTime(startTime);
                    task.setFinishTime(finishTime);
                    task.setTaskType(taskType);
                    task.setTaskStatus(taskStatus);
                    task.setTaskParam(taskParam);
                }
            }
        });

        return task;
    }
}
