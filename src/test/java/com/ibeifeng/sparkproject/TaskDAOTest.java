package com.ibeifeng.sparkproject;

import com.ibeifeng.sparkproject.dao.ITaskDao;
import com.ibeifeng.sparkproject.dao.factory.DAOFactory;
import com.ibeifeng.sparkproject.domain.Task;

/**
 * Created by Administrator on 2018/7/16.
 */
public class TaskDAOTest {

    public static void main (String[] args){
        ITaskDao taskDao = DAOFactory.getTaskDao();
        Task task = taskDao.findById(1);
        System.out.println("============================"+task.getTaskName()+"==========================");
    }
}
