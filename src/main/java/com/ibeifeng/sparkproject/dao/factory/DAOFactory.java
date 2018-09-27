package com.ibeifeng.sparkproject.dao.factory;

import com.ibeifeng.sparkproject.dao.*;
import com.ibeifeng.sparkproject.dao.impl.*;

/**
 * DAO工厂类
 */
public class DAOFactory {
    /**
     * 获取任务管理DAO
     */
    public static ITaskDao getTaskDao(){ return new TaskDaoImpl(); }
    /**
     * 获取session聚合统计DaoI
     * @return
     */
    public static ISessionAggrStatDAO getSessionAggrStatDAO(){
        return new ISessionAggrStatDAOImpl();
    }

    public static ISessionRandomExtractDAO getSessionRandomExtractDAO(){
        return new SessionRandomExtractDAOImpl();
    }

    public static ISessionDetailDAO getSessionDetailDAO(){
        return new SessionDetailDAOImpl();
    }

    public static ITop10CategoryDAO getTop10CategoryDAO(){return new Top10CategoryImpl();}

    public static ITop10SessionDAO getTop10SessionDAO(){return new Top10SessionDAOImpl();}

    public static IPageSplitConverRateDAo getSplitConverRateDAo(){return new PageSplitConverRateDAoImpl();}

    public static IAreaTop3ProductDAO getAreaTop3ProductDAO(){return new AreaTop3ProductDAOImpl();}

    public static IAdUserClickCountDAO getAdUserClickCountDAO(){return new AdUserClickCountDAOImpl();}

    public static IAdBlacklistDao getAdBlacklistDAO(){return new AdBlacklistDaoImpl();}

    public static IAdStatDAO getAdStatDAO(){return new AdStatDAOImpl();}

    public static IAdProvinceTop3DAO getAdProvinceTop3DAO(){return new AdProvinceTop3DAOImpl();}

    public static IAdClickTrendDAO getAdClickTrendDAO(){return new AdClickTrendDAOImpl();}
}

