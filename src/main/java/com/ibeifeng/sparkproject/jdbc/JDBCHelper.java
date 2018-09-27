package com.ibeifeng.sparkproject.jdbc;

import com.ibeifeng.sparkproject.conf.ConfigurationManager;
import com.ibeifeng.sparkproject.constant.Constants;

import java.sql.*;
import java.util.LinkedList;
import java.util.List;


/**
 * JDBC 辅助组件
 */
public class JDBCHelper {

    /**
     * 第一步,在静态代码块直接加载数据库驱动
     */
    static{
        try {
            String driver = ConfigurationManager.getProperty(Constants.JDBC_DRIVER);
            Class.forName(driver);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    /**
     * 第二部,JDBC单例化
     */
    private static JDBCHelper instance = null;
    /**
     * 获取单例
     */
    public static JDBCHelper getinstance(){
        if (instance == null){
            synchronized (JDBCHelper.class){
                if (instance == null){
                    instance = new JDBCHelper();
                }
            }
        }
        return instance;
    }
    //第三部 创建 数据库连接池
     private LinkedList<Connection> datasources = new LinkedList<Connection>();
    /**
     * 私有化构造方法
     */
    private JDBCHelper(){
        //连接池放多少个连接
        int datasourceSize = ConfigurationManager.getInterger(Constants.JDBC_DATASOURCE_SIZE);
        //创建指定数量的数据库连接,并放入数据库连接池中
        for(int i=0 ; i<datasourceSize ;i++){
            String url=null;
            if (ConfigurationManager.getBoolean(Constants.SPARK_LOCAL)){
                url = ConfigurationManager.getProperty(Constants.JDBC_URL);
            }else {
                url = ConfigurationManager.getProperty(Constants.JDBC_URL_SPARK);
            }
            String user = ConfigurationManager.getProperty(Constants.JDBC_USER);
            String psaaword = ConfigurationManager.getProperty(Constants.JDBC_PASSWORD);
            try {
                Connection conn = DriverManager.getConnection(url, user, psaaword);
                datasources.push(conn);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
    /**
    * 第四部,提供获取数据库连接的方法
    */
    public synchronized Connection getConnection(){
        while(datasources.size()==0){
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        return datasources.poll();
    }
    /**
     * 第五部 curd方法
     */
    /**
     *  增删改
     */
    public int executeUpdate(String sql ,Object[] params){
        int rtn =0;
        Connection conn =null;
        PreparedStatement pstmt = null;
        try {
            conn=getConnection();
            pstmt = conn.prepareStatement(sql);
            if (params != null && params.length>0) {
                for (int i = 0; i < params.length; i++) {
                    pstmt.setObject(i + 1, params[i]);
                }
            }
            rtn = pstmt.executeUpdate();
        } catch (Exception e) {
            e.printStackTrace();
        }finally{
            if (conn != null){
                datasources.push(conn);
            }
        }
        return rtn;
    }
    /**
     * 查询
     */
    public void executeQuery(String sql ,Object[] params,QueryCallback callback){
        Connection conn =null;
        PreparedStatement pstmt = null;
        ResultSet rs =null;
        try {
            conn=getConnection();
            pstmt = conn.prepareStatement(sql);
            for (int i=0;i<params.length;i++){
                pstmt.setObject(i+1,params[i]);
            }
            rs = pstmt.executeQuery();
            callback.process(rs);
        } catch (Exception e) {
            e.printStackTrace();
        }finally{
            if (conn != null){
                datasources.push(conn);
            }
        }
    }
    /**
     * 批量执行sql
     */
    public int[] executBatch (String sql, List<Object[]> paramsList){
        int[] rtn = null;
        Connection conn =null;
        PreparedStatement pstmt = null;
        try {
            conn=getConnection();
            //1 使用 Connection对象,取消自动提交
            conn.setAutoCommit(false);
            pstmt = conn.prepareStatement(sql);

            //2 使用PreparedStatement.addBatch加入批量参数
            for (Object[] params : paramsList){
                for(int i=0;i<params.length;i++){
                    pstmt.setObject(i+1,params[i]);
                }
                pstmt.addBatch();
            }
            //3 使用PreparedStatement..executeBatch()执行批量sql语句
            pstmt.executeBatch();
            //4 使用Connection对象,提交批量sql语句
            conn.commit();
        } catch (Exception e) {
            e.printStackTrace();
        }finally{
            if (conn != null){
                datasources.push(conn);
            }
        }
        return null;
    }
    /**
     * 内部类 : 查询回调接口
     */
    public static interface QueryCallback{
        /**
         * 处理查询结果
         */
        void process (ResultSet rs)  throws Exception;
    }
}
